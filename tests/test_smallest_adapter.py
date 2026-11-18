import base64
import json
from pathlib import Path

import httpx
import pytest
import websockets

from vac.adapters.base import TTSRequest, collect_ids
from vac.adapters.smallest import SmallestTTS
from vac.profiles import load_vendor

VENDOR = load_vendor(Path(__file__).resolve().parent.parent / "profiles/vendors/smallest.yaml")
AUDIO = b"\x01\x02" * 50
B64 = base64.b64encode(AUDIO).decode()


def _req():
    return TTSRequest(text="hi", voice="meher", sample_rate=24000, model="lightning_v3.1_pro",
                      client_request_id="vac-r1-smoke-sync-0", speed=1.2, extra={"output_format": "wav"})


def _adapter(handler):
    return SmallestTTS(VENDOR, "plainsecretvalue123", transport=httpx.MockTransport(handler))


def test_body_includes_request_id_and_extra():
    body = SmallestTTS(VENDOR, "k").body(_req())
    assert body == {"text": "hi", "voice_id": "meher", "sample_rate": 24000, "model": "lightning_v3.1_pro",
                    "request_id": "vac-r1-smoke-sync-0", "speed": 1.2, "output_format": "wav"}


async def test_sync_audio_capture_and_header_ids():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers["authorization"]
        seen["url"] = str(request.url)
        return httpx.Response(200, content=AUDIO, headers={"content-type": "audio/wav", "X-Request-Id": "abc",
                                                           "X-External-Request-Id": "vac-r1-smoke-sync-0"})

    async with _adapter(handler) as a:
        cap = await a.synth_sync(_req())
    assert seen == {"auth": "Bearer plainsecretvalue123", "url": "https://api.smallest.ai/waves/v1/tts"}
    assert cap.status == 200 and cap.audio == AUDIO
    assert cap.server_ids == {"x-request-id": "abc", "x-external-request-id": "vac-r1-smoke-sync-0"}
    assert {"t_send", "t_headers", "t_first_byte", "t_end"} <= cap.timings_ms.keys()
    ev = cap.evidence()
    assert ev["audio_len"] == len(AUDIO) and "body_excerpt" not in ev and "body_json" not in ev


async def test_sync_error_body_is_parsed():
    handler = lambda r: httpx.Response(400, json={"error": "bad", "message": "text too long"})
    async with _adapter(handler) as a:
        cap = await a.synth_sync(_req())
    assert cap.status == 400 and cap.audio == b"" and cap.evidence()["body_json"]["error"] == "bad"


async def test_transport_error_is_captured_not_raised():
    def handler(request):
        raise httpx.ConnectError("boom")

    async with _adapter(handler) as a:
        cap = await a.synth_sync(_req())
    assert cap.status is None and cap.error.startswith("ConnectError")


async def test_sse_frames_audio_and_ids():
    stream = (f'event: audio\ndata: {{"audio": "{B64}", "done": false, "status": "206", "request_id": "r-1"}}\n\n'
              f'event: audio\ndata: {{"audio": "{B64}", "done": false, "status": "206", "request_id": "r-1"}}\n\n'
              'data: {"status": "200", "done": true}\n\n')
    handler = lambda r: httpx.Response(200, content=stream.encode(), headers={"content-type": "text/event-stream"})
    async with _adapter(handler) as a:
        cap = await a.synth_sse(_req())
    assert [f["event"] for f in cap.frames] == ["audio", "audio", None]
    assert cap.frames[-1]["data"]["done"] is True
    assert cap.audio == AUDIO * 2 and cap.server_ids == {"request_id": "r-1"}
    frames = cap.evidence()["frames"]
    assert "audio" not in frames[0]["data"] and frames[0]["data"]["audio_b64_len"] == len(B64)


def test_collect_ids_keeps_distinct_values():
    found = {}
    collect_ids(found, {"request_id": "a", "session_id": "s"}, ["request_id", "session_id"])
    collect_ids(found, {"request_id": "b", "session_id": "s"}, ["request_id", "session_id"])
    assert found == {"request_id": ["a", "b"], "session_id": "s"}


@pytest.fixture
async def ws_server():
    async def handler(ws):
        msg = json.loads(await ws.recv())
        if msg.get("type") == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            await ws.wait_closed()
            return
        await ws.send(json.dumps({"status": "chunk", "request_id": "r-9", "session_id": "s-9",
                                  "data": {"audio": B64}}))
        await ws.send(json.dumps({"status": "complete", "request_id": "r-9", "session_id": "s-9"}))
        await ws.close(1000, "done")

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with SmallestTTS({**VENDOR, "base_url": f"http://127.0.0.1:{port}"}, "k") as adapter:
            yield adapter


def test_ws_url_from_https_base():
    assert SmallestTTS(VENDOR, "k").ws_session({"timeout": 5}).capture.url == \
        "wss://api.smallest.ai/waves/v1/tts/live?timeout=5"


async def test_ws_server_closes_after_complete(ws_server):
    adapter = ws_server
    async with adapter.ws_session({"timeout": 5}) as s:
        assert await s.open()
        await s.send_json(adapter.body(_req()))
        done = await s.recv_until(lambda f: f["data"].get("status") == "complete", timeout=5)
        assert done is not None
        assert await s.wait_close(timeout=5)
    cap = s.capture
    assert cap.url.endswith("/tts/live?timeout=5")
    assert cap.handshake_status == 101 and cap.closed_by == "server" and cap.close_code == 1000
    assert cap.audio == AUDIO and cap.server_ids == {"request_id": "r-9", "session_id": "s-9"}
    assert [f["direction"] for f in cap.frames] == ["send", "recv", "recv"]
    assert "audio" not in cap.evidence()["frames"][1]["data"]["data"]


async def test_ws_client_close_is_recorded(ws_server):
    adapter = ws_server
    async with adapter.ws_session({}) as s:
        assert await s.open()
        await s.send_json({"type": "ping"})
        assert (await s.recv(timeout=5))["data"] == {"type": "pong"}
        assert await s.recv(timeout=0.1) is None
    assert s.capture.closed_by == "client" and s.capture.close_code == 1000


async def test_ws_connect_failure_is_captured():
    adapter = SmallestTTS({**VENDOR, "base_url": "http://127.0.0.1:1"}, "k")
    s = adapter.ws_session({})
    assert not await s.open(timeout=2)
    assert s.capture.error and s.capture.closed_by is None
    await adapter.aclose()
