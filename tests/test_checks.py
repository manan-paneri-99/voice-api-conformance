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
