import asyncio
import json
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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
def fast_ws(vendor, monkeypatch):
    """Shrink documented WS timings ~100x so idle/backoff checks run in about a second."""
    claims = vendor["claims"]
    claims["tts.ws.idle_timeout_s"] = {**claims["tts.ws.idle_timeout_s"], "default": 0.6, "max": 1.0}
    claims["tts.ws.complete_backoff_ms"] = {**claims["tts.ws.complete_backoff_ms"], "default": 300}
    monkeypatch.setattr(checks, "TOLERANCE_S", (0.1, 0.3))
    monkeypatch.setattr(checks, "IDLE_GRACE_S", 0.5)
    monkeypatch.setattr(checks.WSIdleTimeout, "PING_AT_S", 0.3)
    monkeypatch.setattr(checks.WSIdleTimeout, "CASES", (
        ("timeout_05", {"timeout": 0.5}, False, False), ("ping_resets", {"timeout": 0.5}, True, False),
        ("default", {}, False, True), ("timeout_2", {"timeout": 2}, False, True)))
    monkeypatch.setattr(checks.WSLifecycle, "CLOSE_WAIT_S", 0.5)
    monkeypatch.setattr(checks.WSLifecycle, "BACKOFFS", (0, 200, None, 10001))
    return vendor


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
        self.idle_default_s, self.idle_max_s = 60, 180
        self.backoff_default_ms = 4000
        self.close_after_complete = True
        self.tokens = {}  # access token -> monotonic expiry
        self.expired_tokens_work = False

    def token_rejected(self, token: str | None) -> bool:
        return bool(token and token.startswith("wat_")) and not self.expired_tokens_work and \
            self.tokens.get(token, 0) < time.monotonic()

    def mint(self, ttl):
        token = f"wat_{secrets.token_hex(8)}"
        self.tokens[token] = time.monotonic() + ttl
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat().replace("+00:00", "Z")
        return httpx.Response(201, json={"access_token": token, "token_type": "Bearer", "expires_in": ttl,
                                         "expires_at": expires_at})

    def process_request(self, connection, request):
        """WS handshake: the token is checked only here, when the connection is made."""
        query = parse_qs(urlparse(request.path).query)
        auth = request.headers.get("Authorization", "")
        token = auth.split(" ", 1)[1] if " " in auth else next((query[k][0] for k in ("api_key", "token") if k in query), None)
        if self.token_rejected(token):
            return connection.respond(401, json.dumps({"error": "Invalid or expired access token"}))
        return None

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
        if not 0 <= body.get("complete_backoff_ms", 0) <= 10000:
            return "bad complete_backoff_ms"
        return None

    async def http(self, request: httpx.Request):
        body = json.loads(request.content)
        if request.url.path.endswith("/auth/token"):
            return self.mint(body["ttl_seconds"])
        if self.token_rejected(request.headers["authorization"].split(" ", 1)[1]):
            return httpx.Response(401, json={"error": "Invalid or expired access token"})
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
        query = parse_qs(urlparse(conn.request.path).query)
        idle = min(float(query["timeout"][0]), self.idle_max_s) if "timeout" in query else self.idle_default_s
        while True:
            try:
                body = json.loads(await asyncio.wait_for(conn.recv(), idle))
            except asyncio.TimeoutError:
                await conn.send(json.dumps({"status": "error",
                                            "message": f"Connection timed out after {idle:g} seconds of inactivity"}))
                await conn.close(1000)
                return
            except websockets.ConnectionClosed:
                return
            if body.get("type") == "ping":
                await conn.send(json.dumps({"type": "pong"}))
                continue
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
                await asyncio.sleep(body.get("complete_backoff_ms", self.backoff_default_ms) / 1000)
                await conn.send(json.dumps({"status": "complete"}))
            finally:
                self.active -= 1
            if self.close_after_complete:
                await conn.close(1000)
                return


@pytest.fixture
async def docs_server(vendor, request):
    server = DocsServer()
    if "fast_ws" in request.fixturenames:
        server.idle_default_s = vendor["claims"]["tts.ws.idle_timeout_s"]["default"]
        server.idle_max_s = vendor["claims"]["tts.ws.idle_timeout_s"]["max"]
        server.backoff_default_ms = vendor["claims"]["tts.ws.complete_backoff_ms"]["default"]
    else:
        server.backoff_default_ms = 0
    async with websockets.serve(server.ws, "127.0.0.1", 0, process_request=server.process_request) as ws:
        port = ws.sockets[0].getsockname()[1]
        local = {**vendor, "base_url": f"http://127.0.0.1:{port}"}
        async with SmallestTTS(local, "plainsecretvalue123", transport=httpx.MockTransport(server.http)) as adapter:
            server.adapter = adapter
            yield server
