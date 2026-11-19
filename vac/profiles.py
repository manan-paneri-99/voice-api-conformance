"""Load and validate vendor and product profiles (YAML)."""

from pathlib import Path

import yaml

TRANSPORTS = ("sync", "sse", "ws")


class ProfileError(ValueError):
    pass


def _require(data: dict, key: str, kind, where: str):
    if not isinstance(data, dict) or key not in data:
        raise ProfileError(f"{where}: missing '{key}'")
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, kind):
        raise ProfileError(f"{where}.{key}: expected {kind}, got {type(value).__name__}")
    return value


def _load(path: Path) -> dict:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ProfileError(f"{path}: top level must be a mapping")
    return data


def load_vendor(path: Path) -> dict:
    data = _load(path)
    where = str(path)
    _require(data, "vendor", str, where)
    if not _require(data, "base_url", str, where).startswith("https://"):
        raise ProfileError(f"{where}.base_url: must be https://")
    auth = _require(data, "auth", dict, where)
    for key in ("header", "scheme", "env"):
        _require(auth, key, str, f"{where}.auth")
    ids = _require(data, "request_id_fields", dict, where)
    for key in ("headers", "frames"):
        if not all(isinstance(f, str) for f in _require(ids, key, list, f"{where}.request_id_fields")):
            raise ProfileError(f"{where}.request_id_fields.{key}: must be a list of strings")
    claims = _require(data, "claims", dict, where)
    for claim_id, claim in claims.items():
        cw = f"{where}.claims.{claim_id}"
        if not isinstance(claim, dict):
            raise ProfileError(f"{cw}: must be a mapping")
        if not _require(claim, "source", str, cw).startswith("https://"):
            raise ProfileError(f"{cw}.source: must be an https:// URL")
        if "value" not in claim and "default" not in claim:
            raise ProfileError(f"{cw}: needs 'value' or 'default'")
    return data


def load_product(path: Path) -> dict:
    data = _load(path)
    where = str(path)
    _require(data, "product", str, where)
    _require(data, "voice", str, where)
    if _require(data, "sample_rate", int, where) <= 0:
        raise ProfileError(f"{where}.sample_rate: must be positive")
    if _require(data, "transport", str, where) not in TRANSPORTS:
        raise ProfileError(f"{where}.transport: must be one of {TRANSPORTS}")
    inputs = _require(data, "inputs", dict, where)
    for name in ("short", "typical"):
        if not _require(inputs, name, str, f"{where}.inputs").strip():
            raise ProfileError(f"{where}.inputs.{name}: must not be blank")
    slo = _require(data, "slo", dict, where)
    if _require(slo, "ttfa_p95_ms", (int, float), f"{where}.slo") <= 0:
        raise ProfileError(f"{where}.slo.ttfa_p95_ms: must be positive")
    if not 0 < _require(slo, "success_rate", (int, float), f"{where}.slo") <= 1:
        raise ProfileError(f"{where}.slo.success_rate: must be in (0, 1]")
    budget = _require(data, "budget", dict, where)
    if _require(budget, "max_chars_total", int, f"{where}.budget") <= 0:
        raise ProfileError(f"{where}.budget.max_chars_total: must be positive")
    if "conversation" in slo:
        for key in ("turn_start_max_ms", "turn_start_p95_ms", "max_underruns", "max_gap_ms", "turn_success_rate"):
            _require(slo["conversation"], key, (int, float), f"{where}.slo.conversation")
    for name in ("conversation", "meeting"):
        if name in data:
            _validate_script(data[name], name == "meeting", f"{where}.{name}")
    return data


def _validate_script(script: dict, meeting: bool, where: str) -> None:
    """Conversation/meeting script: turns with text and pause_s; meeting turns name a speaker and may overlap."""
    if not isinstance(script, dict):
        raise ProfileError(f"{where}: must be a mapping")
    if _require(script, "jitter_buffer_ms", (int, float), where) < 0:
        raise ProfileError(f"{where}.jitter_buffer_ms: must be >= 0")
    speakers = _require(script, "speakers", dict, where) if meeting else {}
    if not meeting and _require(script, "keepalive_every_s", (int, float), where) <= 0:
        raise ProfileError(f"{where}.keepalive_every_s: must be positive")
    turns = _require(script, "turns", list, where)
    if not turns:
        raise ProfileError(f"{where}.turns: must not be empty")
    group: set[str] = set()
    for n, turn in enumerate(turns):
        tw = f"{where}.turns[{n}]"
        if not _require(turn, "text", str, tw).strip():
            raise ProfileError(f"{tw}.text: must not be blank")
        if _require(turn, "pause_s", (int, float), tw) < 0:
            raise ProfileError(f"{tw}.pause_s: must be >= 0")
        if not meeting:
            continue
        speaker = _require(turn, "speaker", str, tw)
        if speaker not in speakers:
            raise ProfileError(f"{tw}.speaker: '{speaker}' is not in {where}.speakers")
        if turn.get("overlap"):
            if n == 0:
                raise ProfileError(f"{tw}.overlap: the first turn cannot overlap")
            if speaker in group:
                raise ProfileError(f"{tw}.overlap: '{speaker}' is already speaking in this overlap")
            group.add(speaker)
        else:
            group = {speaker}
