"""TTS checks. Each reads its expectation from the vendor profile and records expected vs observed per request.

A check has: id, default_runs, estimate(vendor, product, n) -> (requests, chars upper bound), and either
async run(ctx, iteration) -> list of evidence records, or async run_all(ctx, n, emit) that schedules its own
iterations and calls emit(record) as each one finishes.
Outcomes: match | mismatch | inconclusive (the procedure didn't produce the condition it tests) | observed.
"""

import asyncio
import math
from dataclasses import replace

from vac.adapters.base import collect_ids

TRANSPORTS = ("sync", "sse", "ws")
ROUND_PAUSE_S = 2.0
INVALID_SAMPLE_RATE = 22050
UNKNOWN_VOICE = "vac-no-such-voice"
TOLERANCE_S = (0.5, 2.0)  # a timing matches if it lands in [expected - 0.5 s, expected + 2 s]
IDLE_GRACE_S = 15.0  # how long past the expected idle close we keep waiting before closing ourselves


def _terminal(frame: dict) -> bool:
    data = frame.get("data")
    return frame.get("direction") == "recv" and isinstance(data, dict) and data.get("status") in ("complete", "error")


async def _together(*fns):
    """Start all coroutine factories at the same moment (barrier), return their results in order."""
    gate = asyncio.Event()

    async def after_gate(fn):
        await gate.wait()
        return await fn()

    tasks = [asyncio.create_task(after_gate(fn)) for fn in fns]
    await asyncio.sleep(0)
    gate.set()
    return await asyncio.gather(*tasks)


async def _ws_synth(adapter, req, close_wait: float = 10.0, params: dict | None = None):
    """One synthesis on a fresh socket: send, read to complete/error, then wait for the server to close."""
    async with adapter.ws_session(params or {}) as session:
        if await session.open():
            await session.send_json(adapter.body(req))
            if await session.recv_until(_terminal, timeout=30) and close_wait:
                await session.wait_close(close_wait)
    return session.capture


async def _synth(adapter, transport: str, req, close_wait: float = 10.0):
    if transport == "sync":
        return await adapter.synth_sync(req)
    if transport == "sse":
        return await adapter.synth_sse(req)
    return await _ws_synth(adapter, req, close_wait)


def _last_terminal(cap) -> dict | None:
    frames = [f for f in cap.frames if _terminal(f)]
    return frames[-1]["data"] if frames else None


def _error_body(cap) -> dict | None:
    """HTTP error JSON, or for an SSE stream the first frame that looks like an error."""
    if isinstance(cap.json, dict):
        return cap.json
    for f in getattr(cap, "frames", []):
        if isinstance(f["data"], dict) and (f["data"].get("status") == "error" or "error" in f["data"]):
            return f["data"]
    return None


class Smoke:
    id = "smoke"
    default_runs = 1

    def estimate(self, vendor, product, n):
        return 3 * n, 3 * n * len(product["inputs"]["short"])

    async def run(self, ctx, i):
        records = []
        for transport in TRANSPORTS:
            req = ctx.request(self.id, transport, i, ctx.product["inputs"]["short"])
            cap = await _synth(ctx.adapter, transport, req)
            records.append(ctx.record(cap, self.id, i, req, case=transport))
        return records


class Concurrency:
    """Two requests released together, over sync and over two WS sockets."""
    id = "tts.concurrency"
    default_runs = 5

    def estimate(self, vendor, product, n):
        return 4 * n, 4 * n * len(product["inputs"]["typical"])

    async def run(self, ctx, i):
        await asyncio.sleep(ROUND_PAUSE_S)
        records = await self._sync_round(ctx, i)
        await asyncio.sleep(ROUND_PAUSE_S)
        return records + await self._ws_round(ctx, i)

    async def _sync_round(self, ctx, i):
        claim = ctx.claim("tts.concurrency.max_active")
        text = ctx.product["inputs"]["typical"]
        cases = ("sync-a", "sync-b")
        reqs = [ctx.request(self.id, "sync", i, text, case) for case in cases]
        caps = await _together(*(lambda r=r: ctx.adapter.synth_sync(r) for r in reqs))
        a, b = (c.timings_ms for c in caps)
        overlap = a["t_send"] < b["t_end"] and b["t_send"] < a["t_end"]
        expected = {"statuses": sorted([200, claim["on_exceed"]]) if claim["value"] < 2 else [200, 200],
                    "max_active": claim["value"]}
        statuses = [c.status for c in caps]
        observed = {"statuses": sorted(statuses, key=lambda s: (s is None, s or 0)), "overlap": overlap,
                    "send_gap_ms": round(abs(a["t_send"] - b["t_send"]), 1),
                    "retry_after": [c.headers.get("retry-after") for c in caps if c.status not in (200, None)]}
        if not overlap or None in statuses:
            outcome = "inconclusive"
        else:
            outcome = "match" if observed["statuses"] == expected["statuses"] else "mismatch"
        return [ctx.record(c, self.id, i, r, case=case, expected=expected, observed=observed, outcome=outcome)
                for c, r, case in zip(caps, reqs, cases)]

    async def _ws_round(self, ctx, i):
        claim = ctx.claim("tts.concurrency.ws")
        text = ctx.product["inputs"]["typical"]
        cases = ("ws-a", "ws-b")
        reqs = [ctx.request(self.id, "ws", i, text, case) for case in cases]
        sessions = [ctx.adapter.ws_session({}) for _ in reqs]
        terms = [None, None]
        try:
            opened = [await s.open() for s in sessions]
            if all(opened):
                async def go(session, req):
                    await session.send_json(ctx.adapter.body(req))
                    return await session.recv_until(_terminal, timeout=30)
                terms = await _together(*(lambda s=s, r=r: go(s, r) for s, r in zip(sessions, reqs)))
        finally:
            for s in sessions:
                await s.close()
        caps = [s.capture for s in sessions]
        sends = [next((f["t_ms"] for f in c.frames if f["direction"] == "send"), None) for c in caps]
        ends = [t["t_ms"] if t else c.timings_ms.get("t_closed") for t, c in zip(terms, caps)]
        overlap = None not in sends and None not in ends and max(sends) < min(ends)
        statuses = [t["data"]["status"] if t else None for t in terms]
        expected = {"terminal": ["complete", "error"] if claim["value"] < 2 else ["complete", "complete"],
                    "max_active": claim["value"]}
        observed = {"terminal": sorted(statuses, key=lambda s: (s is None, s or "")), "overlap": overlap,
                    "messages": [t["data"].get("message") for t in terms if t and t["data"].get("status") == "error"],
                    "close_codes": [c.close_code for c in caps], "closed_by": [c.closed_by for c in caps]}
        if not all(c.handshake_status == 101 for c in caps) or not overlap:
            outcome = "inconclusive"
        else:
            outcome = "match" if observed["terminal"] == expected["terminal"] else "mismatch"
        return [ctx.record(c, self.id, i, r, case=case, expected=expected, observed=observed, outcome=outcome)
                for c, r, case in zip(caps, reqs, cases)]


def _text_of(length: int, seed: str) -> str:
    """Exactly `length` chars of real sentences, with no leading/trailing whitespace (the limit is after trim)."""
    text = ((seed + " ") * (length // (len(seed) + 1) + 1))[:length]
    return text[:-1] + "." if text[-1].isspace() else text


class Limits:
    """Documented input limits over sync. The at-limit case is billed, so it runs only AT_LIMIT_RUNS times."""
    id = "tts.limits"
    default_runs = 5
    AT_LIMIT_RUNS = 2

    def __init__(self):
        self.over_accepted = False

    def _cases(self, vendor, product, i):
        claims = vendor["claims"]
        max_chars = claims["tts.text.max_chars"]["value"]
        lo, hi = claims["tts.speed.range"]["value"]
        assert INVALID_SAMPLE_RATE not in claims["tts.sample_rate.allowed"]["value"]
        reject = {"status": claims["tts.error.http"]["status"]}
        accept = {"status": 200}
        seed, short = product["inputs"]["typical"], product["inputs"]["short"]
        cases = []
        if i < self.AT_LIMIT_RUNS:
            cases.append(("at_limit", _text_of(max_chars, seed), {}, accept | {"basis": f"max {max_chars} chars"}))
        cases += [
            ("over_limit", _text_of(max_chars + 1, seed), {}, reject | {"basis": f"max {max_chars} chars"}),
            ("whitespace_only", "   \t\n   ", {}, reject | {"basis": "whitespace-only rejected"}),
            (f"speed_{round(lo - 0.1, 2)}", short, {"speed": round(lo - 0.1, 2)},
             reject | {"basis": f"speed range {lo}-{hi}"}),
            (f"speed_{round(hi + 0.1, 2)}", short, {"speed": round(hi + 0.1, 2)},
             reject | {"basis": f"speed range {lo}-{hi}"}),
            (f"sample_rate_{INVALID_SAMPLE_RATE}", short, {"sample_rate": INVALID_SAMPLE_RATE},
             reject | {"basis": "sample_rate enum"}),
        ]
        return cases

    def estimate(self, vendor, product, n):
        max_chars = vendor["claims"]["tts.text.max_chars"]["value"]
        at_limit = min(n, self.AT_LIMIT_RUNS)
        per_iter = [c for c in self._cases(vendor, product, self.AT_LIMIT_RUNS) if c[0] != "over_limit"]
        chars = at_limit * max_chars + (max_chars + 1) + n * sum(len(c[1]) for c in per_iter)
        return at_limit + n + n * len(per_iter), chars

    async def run(self, ctx, i):
        if i == 0:
            self.over_accepted = False
        records = []
        for case, text, overrides, expected in self._cases(ctx.vendor, ctx.product, i):
            if case == "over_limit" and self.over_accepted:
                continue
            req = ctx.request(self.id, "sync", i, text, case, **overrides)
            cap = await ctx.adapter.synth_sync(req)
            observed = {"status": cap.status, "body_keys": sorted(cap.json) if isinstance(cap.json, dict) else None,
                        "audio_len": len(cap.audio), "error": cap.error}
            if cap.status is None:
                outcome = "inconclusive"
            else:
                outcome = "match" if cap.status == expected["status"] else "mismatch"
            if case == "over_limit" and cap.status and 200 <= cap.status < 300:
                self.over_accepted = True
            records.append(ctx.record(cap, self.id, i, req, case=case, expected=expected,
                                      observed=observed, outcome=outcome))
        return records


class ErrorShape:
    """The same invalid request over sync, SSE and WS: compare status, body/frame keys and close behaviour."""
    id = "tts.error_shape"
    default_runs = 5
    CASES = (("sample_rate_22050", {"sample_rate": INVALID_SAMPLE_RATE}), ("unknown_voice", {"voice": UNKNOWN_VOICE}))

    def estimate(self, vendor, product, n):
        per = len(self.CASES) * len(TRANSPORTS)
        return n * per, n * per * len(product["inputs"]["short"])

    async def run(self, ctx, i):
        http_claim, ws_claim = ctx.claim("tts.error.http"), ctx.claim("tts.error.ws_frame")
        records = []
        for case, overrides in self.CASES:
            for transport in TRANSPORTS:
                req = ctx.request(self.id, transport, i, ctx.product["inputs"]["short"], case, **overrides)
                cap = await _synth(ctx.adapter, transport, req, close_wait=5.0)
                if transport == "ws":
                    expected = {"frame_status": ws_claim["value"]["status"], "keys": ws_claim["value"]["keys"]}
                    term = _last_terminal(cap)
                    observed = {"handshake_status": cap.handshake_status,
                                "frame_status": term.get("status") if term else None,
                                "keys": sorted(term) if term else None,
                                "message": term.get("message") if term else None,
                                "close_code": cap.close_code, "close_reason": cap.close_reason,
                                "closed_by": cap.closed_by, "audio_len": len(cap.audio)}
                    ok = observed["frame_status"] == expected["frame_status"] and \
                        set(expected["keys"]) <= set(observed["keys"] or ())
                    responded = term is not None or cap.closed_by is not None
                else:
                    expected = {"status": http_claim["status"], "keys": http_claim["value"]}
                    body = _error_body(cap)
                    observed = {"status": cap.status, "keys": sorted(body) if body else None,
                                "message": body.get("message") if body else None,
                                "content_type": cap.headers.get("content-type"), "audio_len": len(cap.audio)}
                    ok = cap.status == expected["status"] and set(expected["keys"]) <= set(observed["keys"] or ())
                    responded = cap.status is not None
                outcome = "inconclusive" if not responded else ("match" if ok else "mismatch")
                records.append(ctx.record(cap, self.id, i, req, case=f"{case}-{transport}", expected=expected,
                                          observed=observed, outcome=outcome))
        return records


def _within(measured_s: float | None, expected_s: float) -> bool:
    return measured_s is not None and expected_s - TOLERANCE_S[0] <= measured_s <= expected_s + TOLERANCE_S[1]


def _ws_error_message(cap) -> str | None:
    errors = [f["data"] for f in cap.frames
              if f["direction"] == "recv" and isinstance(f["data"], dict) and f["data"].get("status") == "error"]
    return errors[-1].get("message") if errors else None


class WSIdleTimeout:
    """Open a socket and send nothing (or one ping); measure when and how the server closes it."""
    id = "tts.ws_idle_timeout"
    default_runs = 5
    PING_AT_S = 3.0
    # (case, url params, ping?, run the n sockets in parallel?)
    CASES = (("timeout_5", {"timeout": 5}, False, False),
             ("ping_resets", {"timeout": 5}, True, False),
             ("default", {}, False, True),
             ("timeout_200", {"timeout": 200}, False, True))

    def estimate(self, vendor, product, n):
        return n * len(self.CASES), 0

    def _expected_s(self, claim: dict, params: dict) -> float:
        if claim["param"] not in params:
            return claim["default"]
        return min(params[claim["param"]], claim["max"])

    async def run_all(self, ctx, n, emit):
        """Sequential cases one socket at a time; parallel cases hold n idle sockets at once. Each record is
        emitted as soon as its socket closes."""
        claim = ctx.claim("tts.ws.idle_timeout_s")
        for case, params, ping, parallel in self.CASES:
            runs = [self._one(ctx, claim, case, params, ping, i, emit) for i in range(n)]
            if parallel:
                await asyncio.gather(*runs)
            else:
                for run in runs:
                    await run

    async def _one(self, ctx, claim, case, params, ping, i, emit):
        expected_s = self._expected_s(claim, params)
        req = ctx.request(self.id, "ws", i, "", case)
        async with ctx.adapter.ws_session(params) as session:
            if await session.open():
                if ping and not await session.wait_close(self.PING_AT_S):
                    await session.send_json({"type": "ping"})
                await session.wait_close(expected_s + IDLE_GRACE_S)
        cap = session.capture
        sends = [f["t_ms"] for f in cap.frames if f["direction"] == "send"]
        last_client_msg = sends[-1] if sends else cap.timings_ms.get("t_open")
        t_closed = cap.timings_ms.get("t_closed")
        idle_s = round((t_closed - last_client_msg) / 1000, 2) if t_closed and last_client_msg else None
        pong = any(f["direction"] == "recv" and f.get("data") == {"type": "pong"} for f in cap.frames)
        expected = {"idle_close_s": expected_s, "tolerance_s": list(TOLERANCE_S), "closed_by": "server"} | (
            {"pong": True} if ping else {})
        observed = {"idle_s": idle_s, "closed_by": cap.closed_by, "close_code": cap.close_code,
                    "close_reason": cap.close_reason, "error_message": _ws_error_message(cap),
                    "open_to_close_s": round((t_closed - cap.timings_ms["t_open"]) / 1000, 2)
                    if t_closed and "t_open" in cap.timings_ms else None} | ({"pong": pong} if ping else {})
        observed["checks"] = {"timing": _within(idle_s, expected_s), "server_closed": cap.closed_by == "server"} | (
            {"pong": pong} if ping else {})
        if cap.handshake_status != 101:
            outcome = "inconclusive"
        else:
            outcome = "match" if all(observed["checks"].values()) else "mismatch"
        emit(ctx.record(cap, self.id, i, req, case=case, expected=expected, observed=observed, outcome=outcome))


class WSLifecycle:
    """One synthesis per socket with varied complete_backoff_ms: gap to `complete`, then does the server close?"""
    id = "tts.ws_lifecycle"
    default_runs = 5
    CLOSE_WAIT_S = 10.0
    BACKOFFS = (0, 1000, None, 10000, 10001)  # None = omit the field (documented default)

    def estimate(self, vendor, product, n):
        short = len(product["inputs"]["short"])
        return n * (len(self.BACKOFFS) + 1), n * (len(self.BACKOFFS) + 1) * short

    async def run(self, ctx, i):
        claim = ctx.claim("tts.ws.complete_backoff_ms")
        text = ctx.product["inputs"]["short"]
        records = []
        for backoff in self.BACKOFFS:
            case = "backoff_default" if backoff is None else f"backoff_{backoff}"
            extra = {} if backoff is None else {"complete_backoff_ms": backoff}
            req = ctx.request(self.id, "ws", i, text, case, extra=extra)
            reuse_req = ctx.request(self.id, "ws", i, text, f"{case}-reuse") if backoff is None else None
            reuse = None
            async with ctx.adapter.ws_session({}) as session:
                if await session.open():
                    await session.send_json(ctx.adapter.body(req))
                    first = await session.recv_until(_terminal, timeout=30)
                    if first and reuse_req:
                        await session.send_json(ctx.adapter.body(reuse_req))
                        second = await session.recv_until(_terminal, timeout=30)
                        closed_by = session.capture.closed_by
                        reuse = second["data"]["status"] if second else (
                            f"closed_by_{closed_by}" if closed_by else "no_terminal")
                    if first:
                        await session.wait_close(self.CLOSE_WAIT_S)
            cap = session.capture
            invalid = backoff is not None and not 0 <= backoff <= claim["max"]
            observed = self._observe(cap) | ({"reuse": reuse} if reuse_req else {})
            if invalid:
                expected = {"terminal": "error", "basis": f"complete_backoff_ms max {claim['max']}"}
                ok = observed["terminal"] == "error"
            else:
                expected_ms = claim["default"] if backoff is None else backoff
                expected = {"terminal": "complete", "gap_ms": expected_ms, "tolerance_s": list(TOLERANCE_S),
                            "closed_by": "server", "within_s": self.CLOSE_WAIT_S}
                gap = observed["gap_ms"]
                observed["checks"] = {"terminal": observed["terminal"] == "complete",
                                      "gap": gap is not None and _within(gap / 1000, expected_ms / 1000),
                                      "server_closed": cap.closed_by == "server"}
                ok = all(observed["checks"].values())
            outcome = "inconclusive" if cap.handshake_status != 101 else ("match" if ok else "mismatch")
            rec = ctx.record(cap, self.id, i, req, case=case, expected=expected, observed=observed, outcome=outcome)
            if reuse == "complete":
                rec["chars_sent"] += len(reuse_req.text)
            records.append(rec)
        return records

    @staticmethod
    def _observe(cap) -> dict:
        recv = [f for f in cap.frames if f["direction"] == "recv" and isinstance(f.get("data"), dict)]
        first_terminal = next((f for f in recv if _terminal(f)), None)
        chunks = [f for f in recv if f["data"].get("status") == "chunk"
                  and (first_terminal is None or f["t_ms"] < first_terminal["t_ms"])]
        t_closed = cap.timings_ms.get("t_closed")
        return {"terminal": first_terminal["data"]["status"] if first_terminal else None,
                "chunks": len(chunks),
                "gap_ms": round(first_terminal["t_ms"] - chunks[-1]["t_ms"]) if first_terminal and chunks else None,
                "error_message": _ws_error_message(cap),
                "closed_by": cap.closed_by, "close_code": cap.close_code,
                "terminal_to_close_s": round((t_closed - first_terminal["t_ms"]) / 1000, 2)
                if t_closed and first_terminal else None}


def _percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile (with n=10, p95 is the largest sample)."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100 * len(ordered)) - 1)]


def _sse_ttfa_ms(cap) -> float | None:
    start = cap.timings_ms.get("t_request_sent", cap.timings_ms.get("t_send"))
    first = next((f["t_ms"] for f in cap.frames if isinstance(f["data"], dict) and "audio" in f["data"]), None)
    return round(first - start, 1) if first is not None and start is not None else None


def _ws_ttfa_ms(frames: list[dict]) -> float | None:
    send = next((f["t_ms"] for f in frames if f["direction"] == "send"), None)
    first = next((f["t_ms"] for f in frames if f["direction"] == "recv" and isinstance(f.get("data"), dict)
                  and f["data"].get("status") == "chunk"), None)
    return round(first - send, 1) if send is not None and first is not None else None


class TTFA:
    """Time to first audio for the product's typical input, cold and warm, over SSE and WS (spec timing defs)."""
    id = "tts.ttfa"
    default_runs = 10
    CASES = ("sse_cold", "sse_warm", "ws_cold", "ws_warm")

    def estimate(self, vendor, product, n):
        typical, short = len(product["inputs"]["typical"]), len(product["inputs"]["short"])
        return n * len(self.CASES) + 2, n * len(self.CASES) * typical + 2 * short  # +2 warm-ups

    async def run_all(self, ctx, n, emit):
        for case in self.CASES:
            samples = await getattr(self, f"_{case}")(ctx, n, emit)
            emit(self._aggregate(ctx, case, samples))

    def _emit(self, ctx, emit, cap, i, req, case, ttfa_ms, connect_ms, connection):
        observed = {"ttfa_ms": ttfa_ms, "connect_ms": connect_ms, "connection": connection}
        emit(ctx.record(cap, self.id, i, req, case=case, observed=observed, outcome="observed"))
        return ttfa_ms

    async def _sse(self, ctx, n, emit, case, fresh):
        samples = []
        if not fresh:
            req = ctx.request(self.id, "sse", 0, ctx.product["inputs"]["short"], "sse_warmup")
            cap = await ctx.adapter.synth_sse(req)
            self._emit(ctx, emit, cap, 0, req, "sse_warmup", _sse_ttfa_ms(cap), None, cap.connection)
        for i in range(n):
            req = ctx.request(self.id, "sse", i, ctx.product["inputs"]["typical"], case)
            cap = await ctx.adapter.synth_sse(req, fresh=fresh)
            t = cap.timings_ms
            connect_ms = round(t.get("t_tls_end", t["t_connect_end"]) - t["t_connect_start"], 1) \
                if "t_connect_start" in t and "t_connect_end" in t else None
            ttfa = _sse_ttfa_ms(cap) if cap.status == 200 else None
            samples.append(self._emit(ctx, emit, cap, i, req, case, ttfa, connect_ms, cap.connection))
        return samples

    async def _sse_cold(self, ctx, n, emit):
        return await self._sse(ctx, n, emit, "sse_cold", fresh=True)

    async def _sse_warm(self, ctx, n, emit):
        return await self._sse(ctx, n, emit, "sse_warm", fresh=False)

    async def _ws_cold(self, ctx, n, emit):
        samples = []
        for i in range(n):
            req = ctx.request(self.id, "ws", i, ctx.product["inputs"]["typical"], "ws_cold")
            cap = await _ws_synth(ctx.adapter, req, close_wait=0)
            t = cap.timings_ms
            connect_ms = round(t["t_open"] - t["t_connect_start"], 1) if "t_open" in t else None
            samples.append(self._emit(ctx, emit, cap, i, req, "ws_cold", _ws_ttfa_ms(cap.frames), connect_ms, "new"))
        return samples

    async def _ws_warm(self, ctx, n, emit):
        """One socket: a short warm-up synthesis, then n syntheses in a row; one record per synthesis."""
        samples = []
        async with ctx.adapter.ws_session({}) as session:
            if not await session.open():
                return samples
            cap = session.capture
            texts = [("ws_warmup", 0, ctx.product["inputs"]["short"])] + \
                [("ws_warm", i, ctx.product["inputs"]["typical"]) for i in range(n)]
            for case, i, text in texts:
                if cap.closed_by is not None:
                    break
                req = ctx.request(self.id, "ws", i, text, case)
                first_frame, first_audio = len(cap.frames), len(cap.audio)
                await session.send_json(ctx.adapter.body(req))
                await session.recv_until(_terminal, timeout=30)
                frames = cap.frames[first_frame:]
                ids = {}
                for f in frames:
                    if f["direction"] == "recv" and isinstance(f.get("data"), dict):
                        collect_ids(ids, f["data"], session._frame_ids)
                part = replace(cap, frames=frames, audio=cap.audio[first_audio:], server_ids=ids)
                connection = "new" if case == "ws_warmup" else "reused"
                ttfa = self._emit(ctx, emit, part, i, req, case, _ws_ttfa_ms(frames), None, connection)
                if case == "ws_warm":
                    samples.append(ttfa)
        return samples

    def _aggregate(self, ctx, case, samples):
        values = [v for v in samples if v is not None]
        stats = {"n": len(values), "failed": len(samples) - len(values), "method": "nearest-rank",
                 "p50_ms": _percentile(values, 50), "p95_ms": _percentile(values, 95),
                 "min_ms": min(values) if values else None, "max_ms": max(values) if values else None}
        streaming, benchmark = ctx.claim("tts.ttfa_ms"), ctx.claim("tts.ttfa_ms.benchmark")
        slo = ctx.product["slo"]["ttfa_p95_ms"]
        expected = {"claim_streaming_p50_ms": streaming["value"], "claim_benchmark_p50_ms": benchmark["value"],
                    "slo_p95_ms": slo}
        p50, p95 = stats["p50_ms"], stats["p95_ms"]
        checks = {"claim_streaming": p50 is not None and p50 <= streaming["value"],
                  "claim_benchmark": p50 is not None and p50 <= benchmark["value"],
                  "slo_p95": p95 is not None and p95 <= slo}
        outcome = "inconclusive" if not values else ("match" if all(checks.values()) else "mismatch")
        return {"record": "aggregate", "check": self.id, "case": case, "iteration": None, "status": None,
                "server_ids": {}, "error": None, "expected": expected, "observed": stats | {"checks": checks},
                "outcome": outcome}


CHECKS = {c.id: c for c in (Smoke(), Concurrency(), Limits(), ErrorShape(), WSIdleTimeout(), WSLifecycle(), TTFA())}
ALL = ("tts.concurrency", "tts.limits", "tts.error_shape", "tts.ws_idle_timeout", "tts.ws_lifecycle", "tts.ttfa")
