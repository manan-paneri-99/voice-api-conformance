import asyncio
import json
from pathlib import Path

import httpx
import pytest
import websockets
import yaml

from vac.adapters.smallest import SmallestTTS
from vac.checks import tts as checks
from vac.profiles import load_vendor

ROOT = Path(__file__).resolve().parent.parent
B64 = "AQIBAgECAQI="


@pytest.fixture
def vendor():
    return load_vendor(ROOT / "profiles/vendors/smallest.yaml")


@pytest.fixture
def product():
    return yaml.safe_load((ROOT / "profiles/products/example.yaml").read_text())


@pytest.fixture(autouse=True)
def no_pause(monkeypatch):
    monkeypatch.setattr(checks, "ROUND_PAUSE_S", 0)


class DocsServer:
    """Fake vendor that behaves exactly as the docs say: 1 active request, 400 {error, message} on bad input."""

    def __init__(self, max_active=1, delay=0.05):
        self.max_active, self.delay = max_active, delay
        self.active = self.peak = 0

    def invalid(self, body):
        if not body["text"].strip():
            return "text is empty"
        if len(body["text"]) > 8000:
            return "text too long"
        if not 0.5 <= body.get("speed", 1) <= 2.0:
            return "speed out of range"
        if body["sample_rate"] not in (8000, 16000, 24000, 44100):
            return "bad sample_rate"
        if body["voice_id"] == checks.UNKNOWN_VOICE:
            return "unknown voice"
        return None

    async def http(self, request: httpx.Request):
        body = json.loads(request.content)
        if (reason := self.invalid(body)) is not None:
            return httpx.Response(400, json={"error": "bad_request", "message": reason})
        if self.active >= self.max_active:
            return httpx.Response(429, json={"error": "rate_limited", "message": "busy"})
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.active -= 1
        if request.url.path.endswith("/live"):
            sse = f'event: audio\ndata: {{"audio": "{B64}", "done": false}}\n\ndata: {{"done": true}}\n\n'
            return httpx.Response(200, content=sse.encode(), headers={"content-type": "text/event-stream"})
        return httpx.Response(200, content=b"\x01\x02" * 10, headers={"content-type": "audio/wav"})

    async def ws(self, conn):
        body = json.loads(await conn.recv())
        if (reason := self.invalid(body)) is not None:
            await conn.send(json.dumps({"status": "error", "message": reason}))
            await conn.close(1000)
            return
        if self.active >= self.max_active:
            await conn.send(json.dumps({"status": "error", "message": "busy"}))
            await conn.close(1000)
            return
        self.active += 1
        try:
            await asyncio.sleep(self.delay)
            await conn.send(json.dumps({"status": "chunk", "data": {"audio": B64}}))
            await conn.send(json.dumps({"status": "complete"}))
        finally:
            self.active -= 1
        await conn.close(1000)


@pytest.fixture
async def docs_server(vendor):
    server = DocsServer()
    async with websockets.serve(server.ws, "127.0.0.1", 0) as ws:
        port = ws.sockets[0].getsockname()[1]
        local = {**vendor, "base_url": f"http://127.0.0.1:{port}"}
        async with SmallestTTS(local, "plainsecretvalue123", transport=httpx.MockTransport(server.http)) as adapter:
            server.adapter = adapter
            yield server
