import json

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
