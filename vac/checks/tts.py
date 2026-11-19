"""TTS checks. Each reads its expectation from the vendor profile and records expected vs observed per request.

A check has: id, default_runs, estimate(vendor, product, n) -> (requests, chars upper bound), and either
async run(ctx, iteration) -> list of evidence records, or async run_all(ctx, n, emit) that schedules its own
iterations and calls emit(record) as each one finishes.
Outcomes: match | mismatch | inconclusive (the procedure didn't produce the condition it tests) | observed.
"""

import asyncio
import base64
import math
import statistics
import time
from dataclasses import replace
from datetime import datetime, timezone

from vac.adapters.base import _strip_audio, collect_ids
from vac.evidence import now_stamps

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


async def _ws_synth(adapter, req, close_wait: float = 10.0, params: dict | None = None, auth_header: bool = True):
    """One synthesis on a fresh socket: send, read to complete/error, then wait for the server to close."""
    async with adapter.ws_session(params or {}, auth_header=auth_header) as session:
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


def _handshake_body(cap):
    return next((f["data"] for f in cap.frames if f.get("kind") == "handshake_body"), None)


class TokenExpiry:
    """Short-lived access token: query-param names, an open socket across expiry, new requests after expiry.
    Mints one token per iteration (the mint endpoint is rate limited), so it is not part of --all."""
    id = "tts.token_expiry"
    default_runs = 3
    TTL_S = 30
    MARGIN_S = 5.0
    NO_BACKOFF = {"complete_backoff_ms": 0}  # keeps each synthesis short so all valid-token steps fit in the TTL

    def estimate(self, vendor, product, n):
        return 7 * n, 6 * n * len(product["inputs"]["short"])  # upper bound: expired-token requests accepted

    async def run(self, ctx, i):
        short = ctx.product["inputs"]["short"]
        mint = await ctx.adapter.mint_token(self.TTL_S)
        received = time.perf_counter()
        received_utc = datetime.now(timezone.utc)
        body = mint.json if isinstance(mint.json, dict) else {}
        token = body.get("access_token")
        if token:
            ctx.writer.add_secret(token)
        expires_in = body.get("expires_in")
        skew = None
        if body.get("expires_at") and isinstance(expires_in, (int, float)):
            server = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00"))
            skew = round((server - received_utc).total_seconds() - expires_in, 2)
        ttl_claim = ctx.claim("tts.token.ttl_s")
        mint_req = ctx.request(self.id, "mint", i, "", "mint")
        mint_ok = mint.status == 201 and bool(token) and expires_in == self.TTL_S
        records = [ctx.record(mint, self.id, i, mint_req, case="mint",
                              expected={"status": 201, "expires_in": self.TTL_S, "ttl_range": ttl_claim["value"]},
                              observed={"status": mint.status, "token_type": body.get("token_type"),
                                        "token_prefix": token[:4] if token else None, "expires_in": expires_in,
                                        "expires_at": body.get("expires_at"), "server_minus_local_expiry_s": skew,
                                        "ratelimit": {k: v for k, v in mint.headers.items() if k.startswith("ratelimit")}},
                              outcome="match" if mint_ok else ("inconclusive" if mint.status is None else "mismatch"))]
        if not token:
            return records
        tok = ctx.adapter.with_token(token)
        records += [await self._query_param(ctx, tok, token, i, short, claim)
                    for claim in ("tts.token.ws_query_param", "tts.token.ws_query_param_alt")]
        expiry = received + (expires_in if isinstance(expires_in, (int, float)) else self.TTL_S) + self.MARGIN_S
        records.append(await self._open_socket(ctx, tok, i, short, expiry))
        records += [await self._expired_ws(ctx, tok, i, short), await self._expired_sync(ctx, tok, i, short)]
        return records

    async def _query_param(self, ctx, tok, token, i, text, claim_id):
        name = ctx.claim(claim_id)["value"]
        req = ctx.request(self.id, "ws", i, text, f"query_{name}", extra=self.NO_BACKOFF)
        cap = await _ws_synth(tok, req, close_wait=0, params={name: token}, auth_header=False)
        term = _last_terminal(cap)
        observed = {"handshake_status": cap.handshake_status, "terminal": term.get("status") if term else None,
                    "audio_len": len(cap.audio), "error_message": _ws_error_message(cap),
                    "handshake_body": _handshake_body(cap)}
        ok = cap.handshake_status == 101 and observed["terminal"] == "complete" and cap.audio
        return ctx.record(cap, self.id, i, req, case=f"query_{name}", outcome="match" if ok else "mismatch",
                          expected={"handshake_status": 101, "terminal": "complete", "param": name,
                                    "source_claim": claim_id}, observed=observed)

    async def _open_socket(self, ctx, tok, i, text, expiry):
        """Header auth: one synthesis while valid, hold the socket past expiry, then a second synthesis on it."""
        req = ctx.request(self.id, "ws", i, text, "open_socket", extra=self.NO_BACKOFF)
        after_req = ctx.request(self.id, "ws", i, text, "open_socket-after_expiry", extra=self.NO_BACKOFF)
        before = after = None
        closed_during_wait = False
        async with tok.ws_session({}) as session:
            if await session.open():
                await session.send_json(ctx.adapter.body(req))
                first = await session.recv_until(_terminal, timeout=30)
                before = first["data"]["status"] if first else None
                closed_during_wait = await session.wait_close(max(0.0, expiry - time.perf_counter()))
                if not closed_during_wait:
                    await session.send_json(ctx.adapter.body(after_req))
                    second = await session.recv_until(_terminal, timeout=30)
                    after = second["data"]["status"] if second else (
                        f"closed_by_{session.capture.closed_by}" if session.capture.closed_by else "no_terminal")
        cap = session.capture
        claim = ctx.claim("tts.token.ws_survives_expiry")
        observed = {"before_expiry": before, "after_expiry": after, "closed_during_wait": closed_during_wait,
                    "close_code": cap.close_code, "closed_by": cap.closed_by,
                    "error_message": _ws_error_message(cap), "audio_len": len(cap.audio)}
        ok = before == "complete" and after == "complete" and not closed_during_wait
        rec = ctx.record(cap, self.id, i, req, case="open_socket", outcome=(
            "inconclusive" if before != "complete" else ("match" if ok == claim["value"] else "mismatch")),
            expected={"before_expiry": "complete", "after_expiry": "complete" if claim["value"] else "rejected"},
            observed=observed)
        if after == "complete":
            rec["chars_sent"] += len(after_req.text)
        return rec

    async def _expired_ws(self, ctx, tok, i, text):
        claim = ctx.claim("tts.token.expired")
        req = ctx.request(self.id, "ws", i, text, "expired_ws", extra=self.NO_BACKOFF)
        cap = await _ws_synth(tok, req, close_wait=0)
        term = _last_terminal(cap)
        observed = {"handshake_status": cap.handshake_status, "handshake_body": _handshake_body(cap),
                    "terminal": term.get("status") if term else None, "audio_len": len(cap.audio),
                    "error": cap.error}
        ok = cap.handshake_status == claim["value"] and observed["handshake_body"] == claim["body"]
        return ctx.record(cap, self.id, i, req, case="expired_ws", outcome="match" if ok else (
            "inconclusive" if cap.handshake_status is None else "mismatch"),
            expected={"handshake_status": claim["value"], "body": claim["body"]}, observed=observed)

    async def _expired_sync(self, ctx, tok, i, text):
        claim = ctx.claim("tts.token.expired")
        req = ctx.request(self.id, "sync", i, text, "expired_sync")
        cap = await tok.synth_sync(req)
        observed = {"status": cap.status, "body": cap.json, "audio_len": len(cap.audio)}
        ok = cap.status == claim["value"] and cap.json == claim["body"]
        return ctx.record(cap, self.id, i, req, case="expired_sync", outcome="match" if ok else (
            "inconclusive" if cap.status is None else "mismatch"),
            expected={"status": claim["value"], "body": claim["body"]}, observed=observed)


def _now_ms() -> float:
    return time.perf_counter() * 1000


def _clock_offset_ms() -> float:
    """Wall clock minus perf clock. perf_counter stops while the host sleeps; a change in this offset means the host
    slept (or the wall clock was adjusted) and timings across that moment are not trustworthy."""
    return time.time() * 1000 - _now_ms()


def _chunk_audio(frame: dict) -> str | None:
    data = frame.get("data")
    if frame.get("direction") != "recv" or not isinstance(data, dict) or data.get("status") != "chunk":
        return None
    audio = (data.get("data") or {}).get("audio")
    return audio if isinstance(audio, str) else None


def _turn_metrics(frames: list[dict], sample_rate: int, jitter_ms: float, connect_ms: float | None):
    """Per-turn numbers from one turn's frames, plus (playback end, terminal time) in perf ms for scheduling.

    Playback model: 16-bit mono PCM; playback starts at the first chunk + jitter buffer; a chunk arriving after the
    playhead needs it is an underrun (the playhead waits for it)."""
    send = next((f["t_ms"] for f in frames if f["direction"] == "send"
                 and isinstance(f.get("data"), dict) and "text" in f["data"]), None)
    after = [f for f in frames if send is not None and f["t_ms"] >= send]
    chunks = [(f["t_ms"], len(base64.b64decode(a))) for f in after if (a := _chunk_audio(f))]
    term = next((f for f in after if _terminal(f)), None)
    metrics = {"terminal": term["data"]["status"] if term else None, "chunks": len(chunks), "connect_ms": connect_ms,
               "ttfa_ms": None, "turn_start_ms": None, "max_gap_ms": None, "underruns": None, "underrun_ms": None,
               "audio_s": None}
    if not chunks:
        return metrics, None, term["t_ms"] if term else None
    bytes_per_ms = 2 * sample_rate / 1000
    playhead = chunks[0][0] + jitter_ms
    underruns, underrun_ms = 0, 0.0
    for t, n in chunks:
        if t > playhead:
            underruns += 1
            underrun_ms += t - playhead
            playhead = t
        playhead += n / bytes_per_ms
    gaps = [b[0] - a[0] for a, b in zip(chunks, chunks[1:])]
    ttfa = chunks[0][0] - send
    metrics.update(ttfa_ms=round(ttfa, 1), turn_start_ms=round(ttfa + (connect_ms or 0), 1),
                   max_gap_ms=round(max(gaps), 1) if gaps else 0.0, underruns=underruns,
                   underrun_ms=round(underrun_ms, 1), audio_s=round(sum(n for _, n in chunks) / bytes_per_ms / 1000, 2))
    return metrics, playhead, term["t_ms"] if term else None


class Conversation:
    """Scripted multi-turn conversation and a 4-speaker meeting from the product profile; SLOs judged per turn and
    per conversation (see SPEC Step 8). Not part of --all: about 30 minutes of wall time."""
    id = "tts.conversation"
    default_runs = 3
    PARALLEL = 3
    STAGGER_S = 1.5
    TURN_TIMEOUT_S = 60
    CLOCK_JUMP_MS = 2000

    def estimate(self, vendor, product, n):
        conv, meet = product["conversation"]["turns"], product["meeting"]["turns"]
        conversations = 2 * n + self.PARALLEL
        return (conversations * len(conv) + n * len(meet),
                conversations * sum(len(t["text"]) for t in conv) + n * sum(len(t["text"]) for t in meet))

    async def run_all(self, ctx, n, emit):
        for case in ("persistent", "reconnect"):
            for i in range(n):
                await self._conversation(ctx, i, case, emit)
        await asyncio.gather(*(self._conversation(ctx, i, "parallel", emit, start_delay=i * self.STAGGER_S)
                               for i in range(self.PARALLEL)))
        for i in range(n):
            await self._meeting(ctx, i, emit)

    async def _wait_until(self, target_ms: float, session, keepalive_s: float | None) -> None:
        """Wait for the next turn; on a held socket keep receiving (captured) and ping every keepalive_s."""
        while (remaining := (target_ms - _now_ms()) / 1000) > 0:
            if session is None or session.capture.closed_by is not None or not keepalive_s:
                await asyncio.sleep(remaining)
                return
            if not await session.wait_close(min(remaining, keepalive_s)) and target_ms > _now_ms():
                await session.send_json({"type": "ping"})

    @staticmethod
    def _usable(session) -> bool:
        return session is not None and session.capture.handshake_status == 101 and session.capture.closed_by is None

    async def _open(self, ctx):
        session = ctx.adapter.ws_session({})
        ok = await session.open()
        t = session.capture.timings_ms
        return session, (round(t["t_open"] - t["t_connect_start"], 1) if ok else None)

    async def _speak(self, ctx, session, mark: int, req, connect_ms, jitter_ms, clock_base: float):
        """One turn on an open socket. Frames since `mark` (the previous turn's end, so pings and anything the server
        sent while we waited are kept) form this turn's evidence."""
        cap = session.capture
        ts_utc, ts_ist = now_stamps()
        offset_send = _clock_offset_ms()
        await session.send_json(ctx.adapter.body(req))
        await session.recv_until(_terminal, timeout=self.TURN_TIMEOUT_S)
        jump = max(abs(offset_send - clock_base), abs(_clock_offset_ms() - clock_base))
        frames = cap.frames[mark:]
        metrics, playback_end, t_terminal = _turn_metrics(frames, ctx.product["sample_rate"], jitter_ms, connect_ms)
        ids = {}
        for f in frames:
            if f["direction"] == "recv" and isinstance(f.get("data"), dict):
                collect_ids(ids, f["data"], session._frame_ids)
        audio = b"".join(base64.b64decode(a) for f in frames if (a := _chunk_audio(f)))
        part = replace(cap, frames=frames, audio=audio, server_ids=ids, ts_utc=ts_utc, ts_ist=ts_ist)
        metrics["ok"] = metrics["terminal"] == "complete" and metrics["chunks"] > 0
        metrics["clock_jump_ms"] = round(jump)
        metrics["clock_jump"] = jump > self.CLOCK_JUMP_MS
        return part, metrics, playback_end, t_terminal, len(cap.frames)

    @staticmethod
    def _dropped(session, mark: int) -> dict:
        """What a socket that went away between turns sent after the last turn: close details and its final frames."""
        cap = session.capture
        return {"close_code": cap.close_code, "close_reason": cap.close_reason, "closed_by": cap.closed_by,
                "handshake_status": cap.handshake_status, "error": cap.error,
                "frames": [_strip_audio(f) for f in cap.frames[mark:]]}

    def _next_at(self, playback_end, t_terminal, pause_s):
        """Next turn: after playback + the next pause, but not before this turn's `complete` (the backoff)."""
        want = (playback_end or _now_ms()) + pause_s * 1000
        after_complete = t_terminal or _now_ms()
        return max(want, after_complete), round(max(0.0, after_complete - want), 1)

    async def _conversation(self, ctx, i, case, emit, start_delay=0.0):
        script = ctx.product["conversation"]
        turns_cfg = script["turns"]
        await asyncio.sleep(start_delay)
        session, mark, next_at, blocked, reconnects, results = None, 0, None, 0.0, 0, []
        clock_base = _clock_offset_ms()
        try:
            for k, turn in enumerate(turns_cfg):
                if next_at is not None:
                    await self._wait_until(next_at, None if case == "reconnect" else session, script["keepalive_every_s"])
                connect_ms, dropped = None, None
                if not self._usable(session):
                    if session is not None and case != "reconnect":
                        reconnects += 1
                        dropped = self._dropped(session, mark)
                    if session is not None:
                        await session.close()
                    session, connect_ms = await self._open(ctx)
                    mark = 0
                req = ctx.request(self.id, "ws", i, turn["text"], f"{case}-t{k:02d}")
                if not self._usable(session):
                    part, metrics, end, t_term = session.capture, {"ok": False, "terminal": None}, None, None
                else:
                    part, metrics, end, t_term, mark = await self._speak(ctx, session, mark, req, connect_ms,
                                                                         script["jitter_buffer_ms"], clock_base)
                metrics |= {"turn": k, "blocked_by_complete_ms": blocked, "pause_s": turn["pause_s"]}
                if dropped:
                    metrics["previous_socket"] = dropped
                results.append(metrics)
                emit(ctx.record(part, self.id, i, req, case=case, observed=metrics, outcome="observed"))
                if case == "reconnect":
                    await session.close()
                pause_next = turns_cfg[k + 1]["pause_s"] if k + 1 < len(turns_cfg) else 0
                next_at, blocked = self._next_at(end, t_term, pause_next)
        finally:
            if session is not None:
                await session.close()
        emit(self._aggregate(ctx, case, i, results, reconnects))

    async def _meeting(self, ctx, i, emit):
        script = ctx.product["meeting"]
        groups = []
        for k, turn in enumerate(script["turns"]):
            if turn.get("overlap"):
                groups[-1].append((k, turn))
            else:
                groups.append([(k, turn)])
        sessions, marks, reconnects, results = {}, {}, 0, []
        clock_base = _clock_offset_ms()
        try:
            for name in script["speakers"]:
                sessions[name], _ = await self._open(ctx)
                marks[name] = 0
            next_at, blocked = None, 0.0
            for g, group in enumerate(groups):
                if next_at is not None:
                    await self._wait_until(next_at, None, None)

                async def speak(k, turn, blocked=blocked):
                    nonlocal reconnects
                    name = turn["speaker"]
                    connect_ms, dropped = None, None
                    if not self._usable(sessions[name]):
                        reconnects += 1
                        dropped = self._dropped(sessions[name], marks[name])
                        await sessions[name].close()
                        sessions[name], connect_ms = await self._open(ctx)
                        marks[name] = 0
                    req = ctx.request(self.id, "ws", i, turn["text"], f"meeting-t{k:02d}", voice=script["speakers"][name])
                    if self._usable(sessions[name]):
                        part, metrics, end, t_term, marks[name] = await self._speak(
                            ctx, sessions[name], marks[name], req, connect_ms, script["jitter_buffer_ms"], clock_base)
                    else:
                        part, metrics, end, t_term = sessions[name].capture, {"ok": False, "terminal": None}, None, None
                    metrics |= {"turn": k, "speaker": name, "voice": req.voice, "overlap": bool(turn.get("overlap")),
                                "blocked_by_complete_ms": blocked, "pause_s": turn["pause_s"]}
                    if dropped:
                        metrics["previous_socket"] = dropped
                    results.append(metrics)
                    emit(ctx.record(part, self.id, i, req, case="meeting", observed=metrics, outcome="observed"))
                    return end, t_term

                outs = await _together(*(lambda k=k, t=t: speak(k, t) for k, t in group))
                ends = [e for e, _ in outs if e is not None]
                terms = [t for _, t in outs if t is not None]
                pause_next = groups[g + 1][0][1]["pause_s"] if g + 1 < len(groups) else 0
                next_at, blocked = self._next_at(max(ends) if ends else None, max(terms) if terms else None, pause_next)
        finally:
            for s in sessions.values():
                await s.close()
        emit(self._aggregate(ctx, "meeting", i, sorted(results, key=lambda m: m["turn"]), reconnects))

    def _aggregate(self, ctx, case, i, turns, reconnects):
        slo = ctx.product["slo"]["conversation"]
        starts = [t["turn_start_ms"] for t in turns if t["ok"]]
        gaps = [t["max_gap_ms"] for t in turns if t.get("max_gap_ms") is not None]
        underruns = sum(t.get("underruns") or 0 for t in turns)
        success = len(starts) / len(turns) if turns else 0.0
        third = max(1, len(starts) // 3)
        p50 = _percentile(starts, 50)
        observed = {
            "turns": len(turns), "failed": len(turns) - len(starts), "reconnects": reconnects,
            "turn_start_p50_ms": p50, "turn_start_p95_ms": _percentile(starts, 95),
            "turn_start_max_ms": max(starts) if starts else None,
            "ttfa_p50_ms": _percentile([t["ttfa_ms"] for t in turns if t["ok"]], 50),
            "max_gap_ms": max(gaps) if gaps else None, "underruns": underruns,
            "underrun_ms": round(sum(t.get("underrun_ms") or 0 for t in turns), 1),
            "drift_ms": round(statistics.median(starts[-third:]) - statistics.median(starts[:third]), 1)
            if len(starts) >= 3 else None,
            "spread_ms": round(max(starts) - p50, 1) if starts else None,
            "blocked_by_complete_ms": round(sum(t.get("blocked_by_complete_ms") or 0 for t in turns), 1),
            "clock_jumps": sum(bool(t.get("clock_jump")) for t in turns),
        }
        checks = {"turn_start_max": bool(starts) and observed["turn_start_max_ms"] <= slo["turn_start_max_ms"],
                  "turn_start_p95": bool(starts) and observed["turn_start_p95_ms"] <= slo["turn_start_p95_ms"],
                  "underruns": underruns <= slo["max_underruns"],
                  "max_gap": bool(gaps) and observed["max_gap_ms"] <= slo["max_gap_ms"],
                  "turn_success": success >= slo["turn_success_rate"]}
        # a host sleep/clock jump makes the timings untrustworthy, whatever they say
        outcome = "inconclusive" if not starts or observed["clock_jumps"] else (
            "match" if all(checks.values()) else "mismatch")
        return {"record": "aggregate", "check": self.id, "case": case, "iteration": i, "status": None,
                "server_ids": {}, "error": None, "expected": dict(slo), "observed": observed | {"checks": checks},
                "outcome": outcome}


CHECKS = {c.id: c for c in (Smoke(), Concurrency(), Limits(), ErrorShape(), WSIdleTimeout(), WSLifecycle(), TTFA(), TokenExpiry(), Conversation())}
ALL = ("tts.concurrency", "tts.limits", "tts.error_shape", "tts.ws_idle_timeout", "tts.ws_lifecycle", "tts.ttfa")
