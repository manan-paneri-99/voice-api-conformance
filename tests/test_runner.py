import asyncio
import json

import httpx
import pytest

from vac.adapters.base import MAX_CONCURRENCY, TTSRequest
from vac.adapters.smallest import SmallestTTS
from vac.runner import BudgetExceeded, run_checks, spent_chars


def _req(i):
    return TTSRequest(text="hi", voice="meher", sample_rate=24000, model="m", client_request_id=f"c{i}")


async def test_adapter_caps_in_flight_requests(vendor):
    state = {"active": 0, "peak": 0}

    async def handler(request):
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        await asyncio.sleep(0.02)
        state["active"] -= 1
        return httpx.Response(200, content=b"x", headers={"content-type": "audio/wav"})

    async with SmallestTTS(vendor, "k", transport=httpx.MockTransport(handler)) as adapter:
        await asyncio.gather(*(adapter.synth_sync(_req(i)) for i in range(12)))
    assert state["peak"] == MAX_CONCURRENCY


async def test_open_ws_sessions_hold_slots(docs_server):
    adapter = docs_server.adapter
    sessions = [adapter.ws_session({}) for _ in range(MAX_CONCURRENCY)]
    for s in sessions:
        assert await s.open()
    extra = adapter.ws_session({})
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(extra.open(), 0.2)
    for s in sessions:
        await s.close()
    assert await extra.open()
    await extra.close()


def test_spent_chars_counts_only_records_with_audio(tmp_path):
    lines = [{"record": "header"},
             {"record": "request", "chars_sent": 100, "audio_len": 10},
             {"record": "request", "chars_sent": 8001, "audio_len": 0},
             {"record": "request", "chars_sent": 5, "audio_len": 3}]
    (tmp_path / "a.jsonl").write_text("\n".join(json.dumps(r) for r in lines))
    assert spent_chars(tmp_path) == 105


async def test_budget_guard_refuses_before_any_request(docs_server, vendor, product, tmp_path):
    product = {**product, "budget": {"max_chars_total": 100}}
    with pytest.raises(BudgetExceeded):
        await run_checks(docs_server.adapter, vendor, product, ["tts.limits"], 1, "test", (), tmp_path)
    assert list(tmp_path.iterdir()) == []


async def test_run_writes_header_and_masked_records(docs_server, vendor, product, tmp_path):
    path = await run_checks(docs_server.adapter, vendor, product, ["smoke"], 1, "test",
                            ("plainsecretvalue123",), tmp_path)
    recs = [json.loads(line) for line in path.read_text().splitlines()]
    assert recs[0]["record"] == "header" and len(recs) == 4
    assert [r["case"] for r in recs[1:]] == ["sync", "sse", "ws"]
    assert all(r["audio_len"] and r["audio_path"] for r in recs[1:])
    assert "plainsecretvalue123" not in path.read_text()
