import json
import re
from datetime import datetime, timezone

from vac.evidence import MASK, EvidenceWriter, mask, new_run_id, now_stamps

FAKE_KEY = "plainsecretvalue123"


def test_now_stamps_utc_and_ist():
    utc, ist = now_stamps(datetime(2026, 11, 19, 5, 0, 0, tzinfo=timezone.utc))
    assert utc == "2026-11-19T05:00:00.000Z"
    assert ist == "2026-11-19T10:30:00.000+05:30"


def test_run_id_is_sortable_ist():
    run_id = new_run_id(datetime(2026, 11, 19, 5, 0, 0, tzinfo=timezone.utc))
    assert re.fullmatch(r"20261119T103000IST-[0-9a-f]{6}", run_id)


def test_mask_exact_known_secret():
    assert mask(f"key is {FAKE_KEY}!", (FAKE_KEY,)) == f"key is {MASK}!"


def test_mask_patterns():
    assert mask("sk_live_abcdef123456") == MASK
    assert mask("Authorization: Bearer abc.def-123") == f"Authorization: Bearer {MASK}"
    assert mask("wss://h/tts/live?token=abc123&timeout=5") == f"wss://h/tts/live?token={MASK}&timeout=5"
    assert mask("?api_key=xyz") == f"?api_key={MASK}"
    assert mask("jwt eyJhbGciOi.eyJzdWIiOi.c2lnbmF0dXJl") == f"jwt {MASK}"
    assert mask('{"access_token": "wat_AbC123dEf456"}') == f'{{"access_token": "{MASK}"}}'


def test_mask_recurses_and_keeps_non_strings():
    rec = {"headers": {"authorization": f"Bearer {FAKE_KEY}"}, "frames": [{"data": FAKE_KEY}], "status": 429}
    out = mask(rec, (FAKE_KEY,))
    assert FAKE_KEY not in json.dumps(out)
    assert out["status"] == 429


def test_mask_leaves_ordinary_text():
    assert mask("Your order has shipped.") == "Your order has shipped."


def test_writer_header_and_records_are_masked(tmp_path):
    with EvidenceWriter("r1", tmp_path, known_secrets=(FAKE_KEY,)) as w:
        w.write_header(vendor_host="api.smallest.ai", location_note="test")
        w.write({"check": "tts.limits", "status": 400, "body_excerpt": f"bad key {FAKE_KEY}"})
    text = (tmp_path / "r1.jsonl").read_text()
    assert FAKE_KEY not in text
    header, record = [json.loads(line) for line in text.splitlines()]
    assert header["record"] == "header" and header["run_id"] == "r1"
    assert header["tool_version"] == "0.1.0" and header["git_sha"]
    assert record["record"] == "request" and record["body_excerpt"] == f"bad key {MASK}"


def test_writer_masks_secret_added_mid_run(tmp_path):
    with EvidenceWriter("r2", tmp_path) as w:
        w.add_secret("opaque-token-xyz")
        w.write({"body_excerpt": "token opaque-token-xyz"})
    assert "opaque-token-xyz" not in (tmp_path / "r2.jsonl").read_text()
