"""Evidence: one JSON line per request, keys masked, UTC and IST timestamps."""

import json
import re
import secrets
import subprocess
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30), "IST")
MASK = "sk_..."

_PATTERNS = [
    (re.compile(r"sk_[A-Za-z0-9_\-]{8,}"), MASK),
    (re.compile(r"(Bearer\s+)[^\s\"',]+", re.IGNORECASE), r"\g<1>" + MASK),
    (re.compile(r"((?:token|api_key|key)=)[^&\s\"']+", re.IGNORECASE), r"\g<1>" + MASK),
    (re.compile(r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+"), MASK),
]


def now_stamps(now: datetime | None = None) -> tuple[str, str]:
    """Return (ts_utc ending in Z, ts_ist ending in +05:30), millisecond precision."""
    now = now or datetime.now(timezone.utc)
    utc = now.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ist = now.astimezone(IST).isoformat(timespec="milliseconds")
    return utc, ist


def new_run_id(now: datetime | None = None) -> str:
    """Sortable run id, e.g. 20261119T103000IST-a1b2c3."""
    now = now or datetime.now(timezone.utc)
    return f"{now.astimezone(IST):%Y%m%dT%H%M%S}IST-{secrets.token_hex(3)}"


def mask(value, known_secrets: tuple[str, ...] = ()):
    """Recursively mask exact secret values and anything that looks like a key or token."""
    if isinstance(value, str):
        for s in known_secrets:
            if s:
                value = value.replace(s, MASK)
        for pattern, repl in _PATTERNS:
            value = pattern.sub(repl, value)
        return value
    if isinstance(value, dict):
        return {k: mask(v, known_secrets) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask(v, known_secrets) for v in value]
    return value


def git_sha() -> str:
    """HEAD sha, suffixed -dirty if tracked files changed; 'unknown' outside a repo or before the first commit."""
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{sha}-dirty" if dirty else sha


class EvidenceWriter:
    """Appends masked JSONL records to evidence/<run_id>.jsonl, flushing every line."""

    def __init__(self, run_id: str, directory: Path = Path("evidence"), known_secrets: tuple[str, ...] = ()):
        self.run_id = run_id
        self.known_secrets = known_secrets
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{run_id}.jsonl"
        self._file = self.path.open("a", encoding="utf-8")

    def write_header(self, vendor_host: str, location_note: str, **extra) -> None:
        ts_utc, ts_ist = now_stamps()
        self._write({"record": "header", "tool_version": version("voice-api-conformance"), "git_sha": git_sha(),
                     "location_note": location_note, "vendor_host": vendor_host,
                     "ts_utc": ts_utc, "ts_ist": ts_ist, **extra})

    def write(self, record: dict) -> None:
        self._write({"record": "request", **record})

    def masked(self, value):
        return mask(value, self.known_secrets)

    def _write(self, record: dict) -> None:
        line = json.dumps(mask({"run_id": self.run_id, **record}, self.known_secrets), ensure_ascii=False)
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
