import json

from vac.summarize import _window, summarize


def _write(tmp_path, name, records):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in records))
    return path


HEADER = {"record": "header", "run_id": "r1", "git_sha": "abc123", "location_note": "home", "vendor_host": "h",
          "vendor_ips": ["1.2.3.4"], "ts_ist": "2026-11-19T00:57:50.000+05:30"}


def _req(i, **kw):
    return {"record": "request", "run_id": "r1", "check": "tts.limits", "case": "over_limit", "iteration": i,
            "status": 400, "outcome": "match", "ts_ist": f"2026-11-19T00:5{i}:00.000+05:30",
            "timings_ms": {"t_send": 0, "t_end": 60 + i}, "server_ids": {"x-request-id": f"srv-{i}"},
            "client_request_id": f"vac-{i}", "chars_sent": 8001, "audio_len": 0,
            "body_json": {"error": [{"code": "too_big"}]}, "observed": {"status": 400}} | kw


def test_window_formats_report_style():
    recs = [{"ts_ist": "2026-11-19T00:57:50.000+05:30", "timings_ms": {"t_send": 0, "t_end": 1000}},
            {"ts_ist": "2026-11-19T00:58:40.000+05:30", "timings_ms": {"t_send": 0, "t_end": 80_000}}]
    assert _window(recs) == "19/11/2026 00:57–01:00 IST"


def test_groups_by_case_counts_outcomes_and_ids(tmp_path):
    recs = [HEADER, _req(0), _req(1), _req(2, outcome="mismatch", status=200, audio_len=10, body_json=None)]
    out = summarize([_write(tmp_path, "a.jsonl", recs)])
    row = next(line for line in out.splitlines() if line.startswith("| over_limit"))
    assert "| 3 | match 2, mismatch 1 |" in row
    assert "status: 400×2, 200×1" in row and "error code: too_big×2" in row
    assert "`srv-0` (`vac-0`)" in row and "`srv-2` (`vac-2`)" in row
    assert "8001 chars billed" in out and "| `r1` | `abc123` | home | h | 1.2.3.4 |" in out


def test_idle_breakdown_derived_when_checks_missing(tmp_path):
    exp = {"idle_close_s": 5, "tolerance_s": [0.5, 2.0], "closed_by": "server"}
    obs = {"idle_s": 5.02, "closed_by": "transport", "close_code": 1006, "error_message": "timed out"}
    rec = _req(0, check="tts.ws_idle_timeout", case="timeout_5", transport="ws", outcome="mismatch",
               expected=exp, observed=obs)
    out = summarize([_write(tmp_path, "a.jsonl", [HEADER, rec])])
    assert "timing within tolerance (derived): 1/1" in out
    assert "server close handshake (derived): 0/1" in out and "1006/transport×1" in out


def test_ttfa_aggregate_table(tmp_path):
    agg = {"record": "aggregate", "check": "tts.ttfa", "case": "ws_cold", "outcome": "mismatch",
           "expected": {"claim_streaming_p50_ms": 100, "claim_benchmark_p50_ms": 200, "slo_p95_ms": 300},
           "observed": {"n": 10, "p50_ms": 178.0, "p95_ms": 189.2, "min_ms": 159.3, "max_ms": 189.2,
                        "checks": {"claim_streaming": False, "claim_benchmark": True, "slo_p95": True}}}
    req = _req(0, check="tts.ttfa", case="ws_cold", observed={"ttfa_ms": 178.0, "connect_ms": 200.0,
                                                                "connection": "new"}, outcome="observed")
    out = summarize([_write(tmp_path, "a.jsonl", [HEADER, req, agg])])
    assert "| ws_cold | 10 | 178.0 | 189.2 | 159.3–189.2 | no | yes | yes | mismatch |" in out


def test_output_is_masked(tmp_path):
    rec = _req(0, body_json={"error": "bad key sk_live_abcdef123456"}, observed={"status": 400})
    rec["check"], rec["case"] = "tts.error_shape", "x-sync"
    rec["observed"] = {"status": 400, "keys": ["error"], "message": "Bearer abc.def"}
    out = summarize([_write(tmp_path, "a.jsonl", [HEADER, rec])])
    assert "abc.def" not in out and "Bearer sk_..." in out
