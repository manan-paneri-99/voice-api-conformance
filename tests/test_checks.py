import json
import re

from vac.checks.tts import Limits, _text_of
from vac.runner import run_checks


async def _run(server, vendor, product, check, tmp_path, runs=2):
    path = await run_checks(server.adapter, vendor, product, [check], runs, "test", (), tmp_path)
    return [json.loads(line) for line in path.read_text().splitlines()][1:]


def test_text_of_exact_length_no_trailing_space():
    for n in (1, 205, 206, 8000, 8001):
        t = _text_of(n, "Your order has shipped.")
        assert len(t) == n and t == t.strip()


async def test_concurrency_matches_docs_server(docs_server, vendor, product, tmp_path):
    recs = await _run(docs_server, vendor, product, "tts.concurrency", tmp_path)
    assert len(recs) == 8
    assert {r["outcome"] for r in recs} == {"match"}
    sync = [r for r in recs if r["transport"] == "sync"]
    assert sync[0]["observed"]["statuses"] == [200, 429] and sync[0]["observed"]["retry_after"] == [None]


async def test_concurrency_mismatch_when_limit_is_higher(docs_server, vendor, product, tmp_path):
    docs_server.max_active = 10
    recs = await _run(docs_server, vendor, product, "tts.concurrency", tmp_path, runs=1)
    assert {r["outcome"] for r in recs} == {"mismatch"}
    assert recs[0]["observed"]["statuses"] == [200, 200]


async def test_concurrency_inconclusive_without_overlap(docs_server, vendor, product, tmp_path, monkeypatch):
    from vac.checks import tts

    async def one_after_other(*fns):
        return [await fn() for fn in fns]

    monkeypatch.setattr(tts, "_together", one_after_other)
    docs_server.delay = 0
    recs = await _run(docs_server, vendor, product, "tts.concurrency", tmp_path, runs=1)
    assert {r["outcome"] for r in recs} == {"inconclusive"}


async def test_limits_match_docs_server(docs_server, vendor, product, tmp_path):
    recs = await _run(docs_server, vendor, product, "tts.limits", tmp_path, runs=3)
    cases = [r["case"] for r in recs]
    assert cases.count("at_limit") == Limits.AT_LIMIT_RUNS and cases.count("over_limit") == 3
    assert {r["outcome"] for r in recs} == {"match"}
    assert next(r for r in recs if r["case"] == "at_limit")["chars_sent"] == 8000


async def test_limits_over_limit_stops_once_accepted(docs_server, vendor, product, tmp_path):
    docs_server.invalid = lambda body: None
    recs = await _run(docs_server, vendor, product, "tts.limits", tmp_path, runs=3)
    over = [r for r in recs if r["case"] == "over_limit"]
    assert len(over) == 1 and over[0]["outcome"] == "mismatch"


async def test_error_shape_match_docs_server(docs_server, vendor, product, tmp_path):
    recs = await _run(docs_server, vendor, product, "tts.error_shape", tmp_path, runs=1)
    assert len(recs) == 6 and {r["outcome"] for r in recs} == {"match"}
    ws = next(r for r in recs if r["transport"] == "ws")
    assert ws["observed"]["frame_status"] == "error" and ws["observed"]["closed_by"] == "server"


async def test_error_shape_mismatch_on_other_shape(docs_server, vendor, product, tmp_path):
    import httpx

    async def other_shape(request):
        return httpx.Response(400, json={"error_code": "X", "detail": "bad"})

    docs_server.adapter._client._transport = httpx.MockTransport(other_shape)
    recs = await _run(docs_server, vendor, product, "tts.error_shape", tmp_path, runs=1)
    http = [r for r in recs if r["transport"] != "ws"]
    assert {r["outcome"] for r in http} == {"mismatch"} and http[0]["observed"]["keys"] == ["detail", "error_code"]


async def test_ws_idle_timeout_matches_docs_server(fast_ws, docs_server, product, tmp_path):
    recs = await _run(docs_server, fast_ws, product, "tts.ws_idle_timeout", tmp_path, runs=2)
    assert len(recs) == 8 and {r["outcome"] for r in recs} == {"match"}
    by_case = {r["case"]: r for r in recs}
    assert by_case["timeout_2"]["expected"]["idle_close_s"] == 1.0
    assert by_case["ping_resets"]["observed"]["pong"] is True
    assert by_case["ping_resets"]["observed"]["open_to_close_s"] > 0.7
    assert "inactivity" in by_case["default"]["observed"]["error_message"]


async def test_ws_idle_timeout_mismatch_when_not_clamped(fast_ws, docs_server, product, tmp_path):
    docs_server.idle_max_s = 10
    recs = await _run(docs_server, fast_ws, product, "tts.ws_idle_timeout", tmp_path, runs=1)
    clamped = next(r for r in recs if r["case"] == "timeout_2")
    assert clamped["outcome"] == "mismatch" and clamped["observed"]["closed_by"] == "client"


async def test_ws_lifecycle_matches_docs_server(fast_ws, docs_server, product, tmp_path):
    recs = await _run(docs_server, fast_ws, product, "tts.ws_lifecycle", tmp_path, runs=1)
    assert [r["case"] for r in recs] == ["backoff_0", "backoff_200", "backoff_default", "backoff_10001"]
    assert {r["outcome"] for r in recs} == {"match"}
    assert 150 <= recs[1]["observed"]["gap_ms"] <= 500
    assert recs[2]["observed"]["reuse"] == "closed_by_server"
    assert recs[3]["observed"]["terminal"] == "error"


async def test_ws_lifecycle_mismatch_when_server_keeps_socket_open(fast_ws, docs_server, product, tmp_path):
    docs_server.close_after_complete = False
    recs = await _run(docs_server, fast_ws, product, "tts.ws_lifecycle", tmp_path, runs=1)
    default = next(r for r in recs if r["case"] == "backoff_default")
    assert default["outcome"] == "mismatch" and default["observed"]["checks"]["server_closed"] is False
    assert default["observed"]["reuse"] == "complete" and default["chars_sent"] == 2 * len(product["inputs"]["short"])
    assert next(r for r in recs if r["case"] == "backoff_10001")["outcome"] == "match"


async def test_ws_idle_timeout_records_reach_disk_before_check_ends(fast_ws, docs_server, product, tmp_path,
                                                                     monkeypatch):
    from vac.checks import tts

    seen = []
    original = tts.WSIdleTimeout._one

    async def spy(self, ctx, *args):
        seen.append(len(ctx.writer.path.read_text().splitlines()))
        return await original(self, ctx, *args)

    monkeypatch.setattr(tts.WSIdleTimeout, "_one", spy)
    await _run(docs_server, fast_ws, product, "tts.ws_idle_timeout", tmp_path, runs=1)
    assert seen == [1, 2, 3, 4]  # header, then each earlier case's record is on disk before the next starts


def test_percentile_nearest_rank():
    from vac.checks.tts import _percentile

    values = [float(v) for v in range(1, 11)]
    assert _percentile(values, 50) == 5 and _percentile(values, 95) == 10 and _percentile([], 50) is None


async def test_ttfa_records_and_aggregates(docs_server, vendor, product, tmp_path):
    docs_server.close_after_complete = False
    recs = await _run(docs_server, vendor, product, "tts.ttfa", tmp_path, runs=3)
    aggs = [r for r in recs if r["record"] == "aggregate"]
    assert [a["case"] for a in aggs] == ["sse_cold", "sse_warm", "ws_cold", "ws_warm"]
    assert all(a["observed"]["n"] == 3 and a["outcome"] == "match" for a in aggs)
    warm = [r for r in recs if r["case"] == "ws_warm" and r["record"] == "request"]
    assert all(r["server_ids"] == {} for r in warm)  # the fake server echoes no IDs; ours must not leak in
    assert len(warm) == 3 and all(len([f for f in r["frames"] if f["direction"] == "send"]) == 1 for r in warm)
    assert {r["case"] for r in recs} >= {"sse_warmup", "ws_warmup"}


async def test_ttfa_mismatch_when_slow(docs_server, vendor, product, tmp_path):
    docs_server.close_after_complete = False
    docs_server.delay = 0.35
    recs = await _run(docs_server, vendor, product, "tts.ttfa", tmp_path, runs=1)
    agg = next(r for r in recs if r["record"] == "aggregate" and r["case"] == "ws_cold")
    assert agg["outcome"] == "mismatch" and agg["observed"]["checks"]["slo_p95"] is False
    assert agg["observed"]["p50_ms"] >= 350


async def test_token_expiry_matches_docs_server(docs_server, vendor, product, tmp_path, monkeypatch):
    from vac.checks import tts

    monkeypatch.setattr(tts.TokenExpiry, "TTL_S", 1)
    monkeypatch.setattr(tts.TokenExpiry, "MARGIN_S", 0.3)
    docs_server.close_after_complete = False
    path = await run_checks(docs_server.adapter, vendor, product, ["tts.token_expiry"], 1, "test", (), tmp_path)
    text = path.read_text()
    recs = [json.loads(line) for line in text.splitlines()][1:]
    assert [r["case"] for r in recs] == ["mint", "query_api_key", "query_token", "open_socket", "expired_ws",
                                         "expired_sync"]
    assert {r["outcome"] for r in recs} == {"match"}
    assert recs[3]["observed"]["after_expiry"] == "complete" and recs[3]["chars_sent"] == 2 * 23
    assert recs[4]["observed"]["handshake_status"] == 401
    assert not re.search(r"wat_[0-9a-f]{8,}", text)  # minted token masked in body, URL query and everywhere else


async def test_token_expiry_mismatch_when_expired_token_still_works(docs_server, vendor, product, tmp_path,
                                                                    monkeypatch):
    from vac.checks import tts

    monkeypatch.setattr(tts.TokenExpiry, "TTL_S", 1)
    monkeypatch.setattr(tts.TokenExpiry, "MARGIN_S", 0.3)
    docs_server.close_after_complete = False
    docs_server.expired_tokens_work = True
    recs = await _run(docs_server, vendor, product, "tts.token_expiry", tmp_path, runs=1)
    by_case = {r["case"]: r for r in recs}
    assert by_case["expired_ws"]["outcome"] == "mismatch" and by_case["expired_ws"]["observed"]["terminal"] == "complete"
    assert by_case["expired_sync"]["outcome"] == "mismatch" and by_case["open_socket"]["outcome"] == "match"


def test_turn_metrics_playback_model():
    from base64 import b64encode

    from vac.checks.tts import _turn_metrics

    def chunk(t, n):
        return {"t_ms": t, "direction": "recv", "kind": "text",
                "data": {"status": "chunk", "data": {"audio": b64encode(b"\0" * n).decode()}}}

    # sample_rate 1000 Hz -> 2 bytes/ms: each 200-byte chunk is 100 ms of audio; jitter buffer 50 ms
    frames = [{"t_ms": 0, "direction": "send", "kind": "text", "data": {"text": "hi"}},
              chunk(100, 200), chunk(150, 200), chunk(400, 200),
              {"t_ms": 420, "direction": "recv", "kind": "text", "data": {"status": "complete"}}]
    metrics, playback_end, t_terminal = _turn_metrics(frames, 1000, 50, connect_ms=30)
    assert metrics["ttfa_ms"] == 100 and metrics["turn_start_ms"] == 130 and metrics["max_gap_ms"] == 250
    # playhead: starts 150, +100 -> 250, +100 -> 350; third chunk arrives at 400 -> 1 underrun of 50 ms, ends 500
    assert metrics["underruns"] == 1 and metrics["underrun_ms"] == 50 and metrics["audio_s"] == 0.3
    assert (playback_end, t_terminal, metrics["terminal"]) == (500, 420, "complete")


def _short_scripts(product, keepalive_s=0.2, pause_s=0.3):
    product = {**product}
    product["conversation"] = {"jitter_buffer_ms": 0, "keepalive_every_s": keepalive_s, "turns": [
        {"pause_s": 0, "text": "Hello."}, {"pause_s": pause_s, "text": "Second turn."}, {"pause_s": 0.1, "text": "Bye."}]}
    product["meeting"] = {"jitter_buffer_ms": 0, "speakers": {"a": "kelsey", "b": "spencer"}, "turns": [
        {"speaker": "a", "pause_s": 0, "text": "Start."}, {"speaker": "b", "pause_s": 0.1, "text": "Me too."},
        {"speaker": "a", "pause_s": 0, "text": "Together.", "overlap": True}]}
    return product


async def test_conversation_cases_against_docs_server(docs_server, vendor, product, tmp_path, monkeypatch):
    from vac.checks import tts

    monkeypatch.setattr(tts.Conversation, "PARALLEL", 2)
    monkeypatch.setattr(tts.Conversation, "STAGGER_S", 0.05)
    docs_server.close_after_complete = False
    docs_server.max_active = 10
    path = await run_checks(docs_server.adapter, vendor, _short_scripts(product), ["tts.conversation"], 1, "test",
                            (), tmp_path)
    recs = [json.loads(line) for line in path.read_text().splitlines()][1:]
    turns = [r for r in recs if r["record"] == "request"]
    aggs = {r["case"]: r for r in recs if r["record"] == "aggregate"}
    assert sorted(aggs) == ["meeting", "parallel", "persistent", "reconnect"]
    assert all(a["outcome"] == "match" and a["observed"]["failed"] == 0 for a in aggs.values())
    persistent = [r for r in turns if r["case"] == "persistent"]
    reconnect = [r for r in turns if r["case"] == "reconnect"]
    assert [r["observed"]["connect_ms"] is not None for r in persistent] == [True, False, False]
    assert all(r["observed"]["connect_ms"] is not None for r in reconnect)
    assert any(f.get("data") == {"type": "ping"} for f in persistent[1]["frames"])  # keepalive during the pause
    meeting = [r for r in turns if r["case"] == "meeting"]
    assert {r["observed"]["voice"] for r in meeting} == {"kelsey", "spencer"} and len(meeting) == 3
    assert len({r["ts_ist"] for r in persistent}) == 3  # each turn stamped at its own send, not the socket's open
    assert all(r["observed"]["clock_jump"] is False for r in turns)


async def test_conversation_counts_reconnect_when_socket_drops(docs_server, vendor, product, tmp_path, monkeypatch):
    from vac.checks import tts

    monkeypatch.setattr(tts.Conversation, "PARALLEL", 0)
    docs_server.close_after_complete = False
    docs_server.idle_default_s = 0.2
    recs = await _run(docs_server, vendor, _short_scripts(product, keepalive_s=5, pause_s=0.5),
                      "tts.conversation", tmp_path, runs=1)
    persistent = next(r for r in recs if r["record"] == "aggregate" and r["case"] == "persistent")
    assert persistent["observed"]["reconnects"] >= 1 and persistent["observed"]["failed"] == 0
    after_drop = next(r for r in recs if r["case"] == "persistent" and "previous_socket" in r["observed"])
    prev = after_drop["observed"]["previous_socket"]
    assert prev["closed_by"] == "server" and any("inactivity" in json.dumps(f) for f in prev["frames"])


async def test_conversation_inconclusive_on_clock_jump(docs_server, vendor, product, tmp_path, monkeypatch):
    from vac.checks import tts

    calls = {"n": 0}

    def jumping_offset():
        calls["n"] += 1
        return 0.0 if calls["n"] <= 2 else 60_000.0  # the host "slept" a minute after the first turn

    monkeypatch.setattr(tts, "_clock_offset_ms", jumping_offset)
    monkeypatch.setattr(tts.Conversation, "PARALLEL", 0)
    monkeypatch.setattr(tts.Conversation, "run_all", lambda self, ctx, n, emit: self._conversation(
        ctx, 0, "persistent", emit))
    docs_server.close_after_complete = False
    recs = await _run(docs_server, vendor, _short_scripts(product), "tts.conversation", tmp_path, runs=1)
    agg = next(r for r in recs if r["record"] == "aggregate")
    assert agg["outcome"] == "inconclusive" and agg["observed"]["clock_jumps"] >= 1


def test_summarize_conversation_tables(tmp_path):
    from vac.summarize import summarize

    slo = {"turn_start_max_ms": 500, "turn_start_p95_ms": 300, "max_underruns": 0, "max_gap_ms": 250,
           "turn_success_rate": 1.0}
    turn = {"record": "request", "run_id": "r", "check": "tts.conversation", "case": "persistent", "iteration": 0,
            "status": 101, "outcome": "observed", "server_ids": {}, "client_request_id": "c", "error": None,
            "observed": {"turn": 0, "turn_start_ms": 180.0, "ttfa_ms": 180.0, "max_gap_ms": 30.0, "underruns": 0,
                         "underrun_ms": 0, "ok": True, "blocked_by_complete_ms": 0}}
    agg = {"record": "aggregate", "check": "tts.conversation", "case": "persistent", "iteration": 0, "expected": slo,
           "outcome": "mismatch", "observed": {
               "turns": 12, "failed": 0, "reconnects": 0, "turn_start_p50_ms": 180, "turn_start_p95_ms": 320,
               "turn_start_max_ms": 320, "max_gap_ms": 30, "underruns": 0, "drift_ms": 5, "blocked_by_complete_ms": 0,
               "checks": {"turn_start_max": True, "turn_start_p95": False, "underruns": True, "max_gap": True,
                          "turn_success": True}}}
    path = tmp_path / "a.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in [turn, agg]))
    out = summarize([path])
    assert "| persistent | 0 | 12 | 0 | 0 | 180 / 320 / 320 | 30 | 0 | 5 | 0 | 0 | turn_start_p95 | mismatch |" in out
    assert "| 0 | 180 |" in out
