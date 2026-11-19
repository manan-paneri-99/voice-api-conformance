"""Adapter interface and raw capture types. Adapters capture; checks interpret.

All t_* values are time.perf_counter() in milliseconds, so captures from the same
process can be compared (e.g. overlap of two parallel requests).

HTTP timings: t_send (adapter call, slot held), t_connect_start/t_connect_end/t_tls_end (only on a new
connection), t_request_sent (request body written), t_headers, t_first_byte, t_end.
TTFA (time to first audio) = first audio frame - t_request_sent (HTTP) or - the text frame's send time (WS).
Connect time is recorded separately and never folded into TTFA.
"""

import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol

BODY_EXCERPT_CHARS = 2000
MAX_CONCURRENCY = 5  # hard cap on in-flight requests + open WS sessions per adapter; not configurable


@dataclass
class TTSRequest:
    text: str
    voice: str
    sample_rate: int
    model: str
    client_request_id: str
    speed: float | None = None
    extra: dict = field(default_factory=dict)


@dataclass
class HttpCapture:
    transport: str
    url: str
    ts_utc: str = ""
    ts_ist: str = ""
    status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    json: Any = None
    timings_ms: dict[str, float] = field(default_factory=dict)
    server_ids: dict[str, Any] = field(default_factory=dict)
    audio: bytes = b""
    peer_ip: str | None = None
    error: str | None = None

    @property
    def connection(self) -> str | None:
        """'new' or 'reused', from connection trace events; None when the transport emitted none (tests)."""
        if "t_request_sent" not in self.timings_ms:
            return None
        return "new" if "t_connect_start" in self.timings_ms else "reused"

    def evidence(self) -> dict:
        """Fields for an evidence record. Audio is summarised here; the raw bytes stay on the capture."""
        out = {
            "transport": self.transport, "url": self.url, "ts_utc": self.ts_utc, "ts_ist": self.ts_ist,
            "status": self.status, "headers": self.headers, "timings_ms": self.timings_ms,
            "connection": self.connection, "peer_ip": self.peer_ip,
            "server_ids": self.server_ids, "audio_len": len(self.audio),
            "audio_sha256": hashlib.sha256(self.audio).hexdigest() if self.audio else None,
            "error": self.error,
        }
        if self.json is not None:
            out["body_json"] = self.json
        elif self.body and not self.audio:
            out["body_excerpt"] = self.body[:BODY_EXCERPT_CHARS].decode("utf-8", "replace")
        return out


@dataclass
class StreamCapture(HttpCapture):
    frames: list[dict] = field(default_factory=list)

    def evidence(self) -> dict:
        return {**super().evidence(), "frames": [_strip_audio(f) for f in self.frames]}


@dataclass
class WSCapture:
    transport = "ws"
    url: str
    ts_utc: str = ""
    ts_ist: str = ""
    handshake_status: int | None = None
    handshake_headers: dict[str, str] = field(default_factory=dict)
    timings_ms: dict[str, float] = field(default_factory=dict)
    frames: list[dict] = field(default_factory=list)
    close_code: int | None = None
    close_reason: str | None = None
    closed_by: str | None = None
    server_ids: dict[str, Any] = field(default_factory=dict)
    audio: bytes = b""
    peer_ip: str | None = None
    error: str | None = None

    def evidence(self) -> dict:
        return {
            "transport": self.transport, "url": self.url, "ts_utc": self.ts_utc, "ts_ist": self.ts_ist,
            "peer_ip": self.peer_ip, "status": self.handshake_status, "headers": self.handshake_headers, "timings_ms": self.timings_ms,
            "frames": [_strip_audio(f) for f in self.frames], "close_code": self.close_code,
            "close_reason": self.close_reason, "closed_by": self.closed_by, "server_ids": self.server_ids,
            "audio_len": len(self.audio),
            "audio_sha256": hashlib.sha256(self.audio).hexdigest() if self.audio else None,
            "error": self.error,
        }


def _strip_audio(frame: dict) -> dict:
    """Replace base64 audio in a frame's parsed data with its length."""
    data = frame.get("data")
    if not isinstance(data, dict):
        return frame
    inner = data.get("data") if isinstance(data.get("data"), dict) else data
    if not isinstance(inner.get("audio"), str):
        return frame
    stripped = {k: v for k, v in inner.items() if k != "audio"} | {"audio_b64_len": len(inner["audio"])}
    data = {**data, "data": stripped} if inner is not data else stripped
    return {**frame, "data": data}


def collect_ids(found: dict[str, Any], source: dict, fields: list[str]) -> None:
    """Record values of the named fields (case-insensitive) into found; repeats with new values become lists."""
    lowered = {str(k).lower(): v for k, v in source.items()}
    for name in fields:
        value = lowered.get(name.lower())
        if value is None:
            continue
        key = name.lower()
        if key not in found:
            found[key] = value
        elif isinstance(found[key], list):
            if value not in found[key]:
                found[key].append(value)
        elif found[key] != value:
            found[key] = [found[key], value]


class WSSession(Protocol):
    capture: WSCapture
    async def open(self, timeout: float = 10) -> bool: ...
    async def send_json(self, obj: dict) -> None: ...
    async def recv(self, timeout: float) -> dict | None: ...
    async def recv_until(self, predicate, timeout: float) -> dict | None: ...
    async def wait_close(self, timeout: float) -> bool: ...
    async def close(self) -> None: ...


class TTSAdapter(Protocol):
    name: str
    def body(self, req: TTSRequest) -> dict: ...
    async def synth_sync(self, req: TTSRequest, fresh: bool = False) -> HttpCapture: ...
    async def synth_sse(self, req: TTSRequest, fresh: bool = False) -> StreamCapture: ...
    def ws_session(self, url_params: dict, auth_header: bool = True) -> WSSession: ...
    def with_token(self, token: str) -> "TTSAdapter": ...
    async def mint_token(self, ttl_seconds: int) -> HttpCapture: ...
