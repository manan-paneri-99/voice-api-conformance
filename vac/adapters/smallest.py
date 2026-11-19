"""Smallest TTS adapter (Waves/Models API): sync HTTP, SSE and WebSocket."""

import asyncio
import base64
import copy
import json
import time
from urllib.parse import urlencode, urlparse

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from vac.adapters.base import MAX_CONCURRENCY, HttpCapture, StreamCapture, TTSRequest, WSCapture, collect_ids
from vac.evidence import now_stamps

HTTP_TIMEOUT = httpx.Timeout(120.0, connect=10.0)  # at-limit (8000 char) sync synthesis is slow


def _ms() -> float:
    return time.perf_counter() * 1000


def _parse_json(raw: bytes | str):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None


_TRACE_TIMINGS = {"connection.connect_tcp.started": "t_connect_start",
                  "connection.connect_tcp.complete": "t_connect_end",
                  "connection.start_tls.complete": "t_tls_end",
                  "http11.send_request_body.complete": "t_request_sent"}


def _tracer(cap: HttpCapture):
    """httpcore trace hook: records connection and request-sent times on the capture."""
    async def trace(event: str, info: dict) -> None:
        if event in _TRACE_TIMINGS:
            cap.timings_ms[_TRACE_TIMINGS[event]] = _ms()
    return trace


class SmallestTTS:
    name = "smallest"

    def __init__(self, vendor: dict, api_key: str, transport: httpx.AsyncBaseTransport | None = None):
        self.base_url = vendor["base_url"].rstrip("/")
        self.host = urlparse(self.base_url).hostname
        self.token_url = vendor.get("token_url")
        self._auth = vendor["auth"]
        self._auth_headers = {self._auth["header"]: f"{self._auth['scheme']} {api_key}"}
        self._header_ids = vendor["request_id_fields"]["headers"]
        self._frame_ids = vendor["request_id_fields"]["frames"]
        self._transport = transport
        self._client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, transport=transport)
        self._slots = asyncio.Semaphore(MAX_CONCURRENCY)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    def with_token(self, token: str) -> "SmallestTTS":
        """Same client and concurrency slots, authenticating with an access token instead of the key.
        Don't close the returned adapter; it shares the parent's HTTP client."""
        clone = copy.copy(self)
        clone._auth_headers = {self._auth["header"]: f"{self._auth['scheme']} {token}"}
        return clone

    async def mint_token(self, ttl_seconds: int) -> HttpCapture:
        """POST the vendor's token endpoint with the API key; the parsed body holds access_token/expires_in."""
        cap = HttpCapture(transport="mint", url=self.token_url)
        await self._post(cap, {"ttl_seconds": ttl_seconds}, self._read_sync, False)
        return cap

    def body(self, req: TTSRequest) -> dict:
        body = {"text": req.text, "voice_id": req.voice, "sample_rate": req.sample_rate,
                "model": req.model, "request_id": req.client_request_id}
        if req.speed is not None:
            body["speed"] = req.speed
        return body | req.extra

    async def synth_sync(self, req: TTSRequest, fresh: bool = False) -> HttpCapture:
        """fresh=True sends on a new connection (cold); otherwise the shared client may reuse one (warm)."""
        cap = HttpCapture(transport="sync", url=f"{self.base_url}/tts")
        await self._post(cap, self.body(req), self._read_sync, fresh)
        return cap

    async def synth_sse(self, req: TTSRequest, fresh: bool = False) -> StreamCapture:
        cap = StreamCapture(transport="sse", url=f"{self.base_url}/tts/live")
        await self._post(cap, self.body(req), self._read_sse, fresh)
        return cap

    def ws_session(self, url_params: dict, auth_header: bool = True) -> "SmallestWSSession":
        """auth_header=False sends no Authorization header (auth via a query parameter in url_params)."""
        url = "ws" + self.base_url.removeprefix("http") + "/tts/live"
        if url_params:
            url += "?" + urlencode(url_params)
        return SmallestWSSession(url, self._auth_headers if auth_header else {}, self._frame_ids, self._slots)

    async def _post(self, cap: HttpCapture, payload: dict, read_body, fresh: bool) -> None:
        async with self._slots:
            client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, transport=self._transport) if fresh else self._client
            cap.ts_utc, cap.ts_ist = now_stamps()
            cap.timings_ms["t_send"] = _ms()
            try:
                async with client.stream("POST", cap.url, json=payload, headers=self._auth_headers,
                                         extensions={"trace": _tracer(cap)}) as resp:
                    cap.timings_ms["t_headers"] = _ms()
                    stream = resp.extensions.get("network_stream")
                    addr = stream.get_extra_info("server_addr") if stream else None
                    cap.peer_ip = addr[0] if addr else None
                    cap.status = resp.status_code
                    cap.headers = dict(resp.headers)
                    collect_ids(cap.server_ids, cap.headers, self._header_ids)
                    await read_body(cap, resp)
            except httpx.HTTPError as exc:
                cap.error = f"{type(exc).__name__}: {exc}"
            finally:
                if fresh:
                    await client.aclose()
            cap.timings_ms["t_end"] = _ms()

    async def _read_sync(self, cap: HttpCapture, resp: httpx.Response) -> None:
        chunks = []
        async for chunk in resp.aiter_bytes():
            cap.timings_ms.setdefault("t_first_byte", _ms())
            chunks.append(chunk)
        cap.body = b"".join(chunks)
        if resp.headers.get("content-type", "").startswith("audio/") and resp.is_success:
            cap.audio = cap.body
        else:
            cap.json = _parse_json(cap.body)
            if cap.json is None and resp.is_success:
                cap.audio = cap.body

    async def _read_sse(self, cap: StreamCapture, resp: httpx.Response) -> None:
        if not resp.headers.get("content-type", "").startswith("text/event-stream"):
            await self._read_sync(cap, resp)
            return
        event, data_lines, raw = None, [], []
        async for line in resp.aiter_lines():
            cap.timings_ms.setdefault("t_first_byte", _ms())
            raw.append(line)
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif line == "" and (event or data_lines):
                self._sse_frame(cap, event, "\n".join(data_lines))
                event, data_lines = None, []
        if event or data_lines:
            self._sse_frame(cap, event, "\n".join(data_lines))
        cap.body = "\n".join(raw).encode()

    def _sse_frame(self, cap: StreamCapture, event: str | None, payload: str) -> None:
        data = _parse_json(payload)
        cap.frames.append({"t_ms": _ms(), "event": event, "data": data if data is not None else payload})
        if isinstance(data, dict):
            collect_ids(cap.server_ids, data, self._frame_ids)
            if isinstance(data.get("audio"), str):
                cap.audio += base64.b64decode(data["audio"])


class SmallestWSSession:
    """One WebSocket connection; every frame in either direction is captured with a timestamp."""

    def __init__(self, url: str, headers: dict, frame_id_fields: list[str], slots: asyncio.Semaphore):
        self.capture = WSCapture(url=url)
        self._headers = headers
        self._frame_ids = frame_id_fields
        self._slots = slots
        self._holding = False
        self._ws = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def open(self, timeout: float = 10) -> bool:
        """Connect; the session holds one concurrency slot from here until it closes."""
        await self._slots.acquire()
        self._holding = True
        cap = self.capture
        cap.ts_utc, cap.ts_ist = now_stamps()
        cap.timings_ms["t_connect_start"] = _ms()
        try:
            self._ws = await websockets.connect(cap.url, additional_headers=self._headers,
                                                open_timeout=timeout, ping_interval=None)
        except InvalidStatus as exc:
            cap.handshake_status = exc.response.status_code
            cap.handshake_headers = dict(exc.response.headers.raw_items())
            cap.frames.append({"t_ms": _ms(), "direction": "recv", "kind": "handshake_body",
                               "data": _parse_json(exc.response.body) or exc.response.body.decode("utf-8", "replace")})
            cap.error = f"InvalidStatus: {exc}"
            self._release()
            return False
        except (OSError, asyncio.TimeoutError, websockets.exceptions.InvalidHandshake) as exc:
            cap.error = f"{type(exc).__name__}: {exc}"
            self._release()
            return False
        cap.timings_ms["t_open"] = _ms()
        cap.peer_ip = (self._ws.remote_address or (None,))[0]
        cap.handshake_status = self._ws.response.status_code
        cap.handshake_headers = dict(self._ws.response.headers.raw_items())
        return True

    async def send_json(self, obj: dict) -> None:
        raw = json.dumps(obj)
        self.capture.frames.append({"t_ms": _ms(), "direction": "send", "kind": "text", "data": obj})
        try:
            await self._ws.send(raw)
        except ConnectionClosed:
            self._record_close()

    async def recv(self, timeout: float) -> dict | None:
        """Next received frame, or None on timeout or close."""
        try:
            msg = await asyncio.wait_for(self._ws.recv(), timeout)
        except asyncio.TimeoutError:
            return None
        except ConnectionClosed:
            self._record_close()
            return None
        t = _ms()
        if isinstance(msg, bytes):
            frame = {"t_ms": t, "direction": "recv", "kind": "binary", "len": len(msg)}
        else:
            data = _parse_json(msg)
            frame = {"t_ms": t, "direction": "recv", "kind": "text", "data": data if data is not None else msg}
            if isinstance(data, dict):
                collect_ids(self.capture.server_ids, data, self._frame_ids)
                inner = data.get("data")
                if isinstance(inner, dict) and isinstance(inner.get("audio"), str):
                    self.capture.audio += base64.b64decode(inner["audio"])
        self.capture.frames.append(frame)
        return frame

    async def recv_until(self, predicate, timeout: float) -> dict | None:
        """Receive until predicate(frame) is true; None if the deadline passes or the socket closes first."""
        deadline = _ms() + timeout * 1000
        while (remaining := (deadline - _ms()) / 1000) > 0:
            frame = await self.recv(remaining)
            if frame is None:
                return None
            if predicate(frame):
                return frame
        return None

    async def wait_close(self, timeout: float) -> bool:
        """Keep receiving (and capturing) until the server closes; False if still open after timeout."""
        deadline = _ms() + timeout * 1000
        while self.capture.closed_by is None and (remaining := (deadline - _ms()) / 1000) > 0:
            await self.recv(remaining)
        return self.capture.closed_by is not None

    async def close(self) -> None:
        if self._ws is not None and self.capture.closed_by is None:
            await self._ws.close()
            self._record_close()

    def _release(self) -> None:
        if self._holding:
            self._holding = False
            self._slots.release()

    def _record_close(self) -> None:
        cap = self.capture
        self._release()
        if cap.closed_by is not None:
            return
        cap.timings_ms["t_closed"] = _ms()
        protocol = self._ws.protocol
        cap.close_code = protocol.close_code
        cap.close_reason = protocol.close_reason
        if protocol.close_rcvd_then_sent is True:
            cap.closed_by = "server"
        elif protocol.close_rcvd_then_sent is False:
            cap.closed_by = "client"
        else:
            cap.closed_by = "transport"
