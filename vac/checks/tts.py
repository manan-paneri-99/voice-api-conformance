"""TTS checks. Each reads its expectation from the vendor profile and records expected vs observed per request.

A check has: id, default_runs, estimate(vendor, product, n) -> (requests, chars upper bound),
and async run(ctx, iteration) -> list of evidence records.
Outcomes: match | mismatch | inconclusive (the procedure didn't produce the condition it tests) | observed.
"""

import asyncio

TRANSPORTS = ("sync", "sse", "ws")
ROUND_PAUSE_S = 2.0
INVALID_SAMPLE_RATE = 22050
UNKNOWN_VOICE = "vac-no-such-voice"


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


CHECKS = {c.id: c for c in (Smoke(), Concurrency(), Limits(), ErrorShape())}
ALL = ("tts.concurrency", "tts.limits", "tts.error_shape")
