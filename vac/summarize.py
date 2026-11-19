"""Evidence JSONL -> markdown: per check and case, n, outcome counts, key numbers, IST window, example IDs.

Numbers only. Stored outcomes are shown as recorded; anything recomputed here is labelled "derived".
"""

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from vac.checks.tts import ALL, _percentile
from vac.evidence import mask

ORDER = ("smoke",) + ALL + ("tts.token_expiry",)
ID_FIELDS = ("x-request-id", "request_id")


def load(paths: list[Path]) -> tuple[list[dict], list[dict]]:
    headers, records = [], []
    for path in paths:
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            (headers if rec.get("record") == "header" else records).append(rec)
    return headers, records


def _ist(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _duration_ms(rec: dict) -> float | None:
    t = rec.get("timings_ms") or {}
    if "t_send" in t and "t_end" in t:
        return t["t_end"] - t["t_send"]
    if "t_connect_start" in t and "t_closed" in t:
        return t["t_closed"] - t["t_connect_start"]
    return None


def _window(recs: list[dict]) -> str:
    """IST window from the first request start to the last request end, report-form format."""
    starts = [_ist(r["ts_ist"]) for r in recs if r.get("ts_ist")]
    if not starts:
        return ""
    ends = [_ist(r["ts_ist"]) + timedelta(milliseconds=d) for r in recs if r.get("ts_ist")
            and (d := _duration_ms(r)) is not None] or starts
    start, end = min(starts), max(ends)
    fmt_end = "%H:%M" if start.date() == end.date() else "%d/%m/%Y %H:%M"
    return f"{start:%d/%m/%Y %H:%M}–{end.strftime(fmt_end)} IST"


def _server_id(rec: dict) -> str | None:
    ids = rec.get("server_ids") or {}
    for field in ID_FIELDS:
        value = ids.get(field)
        if value:
            return value[0] if isinstance(value, list) else value
    return None


def _example_ids(recs: list[dict]) -> str:
    picks = [recs[0]] if len(recs) == 1 else [recs[0], recs[-1]]
    return "<br>".join(f"`{_server_id(r) or '-'}` (`{r.get('client_request_id')}`)" for r in picks)


def _stats(values: list[float], unit: str = "ms") -> str:
    values = [v for v in values if v is not None]
    if not values:
        return "-"
    return (f"p50 {_percentile(values, 50):g} / p95 {_percentile(values, 95):g} / "
            f"min {min(values):g} / max {max(values):g} {unit} (n={len(values)})")


def _counts(values) -> str:
    return ", ".join(f"{k}×{v}" for k, v in Counter(json.dumps(x) if isinstance(x, (list, dict)) else str(x)
                                                     for x in values).most_common())


def _obs(recs, key):
    return [(r.get("observed") or {}).get(key) for r in recs]


def _numbers(check: str, recs: list[dict]) -> list[str]:
    """Key numbers per check, as 'label: value' strings."""
    durations = [round(d) for r in recs if (d := _duration_ms(r)) is not None]
    if check == "smoke":
        return [f"status: {_counts(r['status'] for r in recs)}",
                f"audio bytes: {_counts(r.get('audio_len') for r in recs)}"]
    if check == "tts.concurrency":
        rounds = {r["iteration"]: r["observed"] for r in recs}
        key = "statuses" if "statuses" in next(iter(rounds.values())) else "terminal"
        out = [f"rounds: {len(rounds)}", f"{key} per round: {_counts(o[key] for o in rounds.values())}",
               f"overlapped: {sum(bool(o['overlap']) for o in rounds.values())}/{len(rounds)}",
               f"duration: {_stats(durations)}"]
        if key == "statuses":
            out.append(f"Retry-After on non-200: {_counts(v for o in rounds.values() for v in o['retry_after']) or 'none'}")
        else:
            out.append(f"closed_by: {_counts(c for o in rounds.values() for c in o['closed_by'])}")
        return out
    if check == "tts.limits":
        codes = [(r.get("body_json") or {}).get("error", [{}])[0].get("code") if isinstance(
            (r.get("body_json") or {}).get("error"), list) else None for r in recs]
        return [f"status: {_counts(r['status'] for r in recs)}", f"error code: {_counts(codes)}",
                f"duration: {_stats(durations)}"]
    if check == "tts.error_shape":
        out = [f"status: {_counts(r['status'] for r in recs)}", f"keys: {_counts(_obs(recs, 'keys'))}",
               f"message: {_counts(_obs(recs, 'message'))}"]
        if recs[0].get("transport") == "ws":
            out += [f"frame status: {_counts(_obs(recs, 'frame_status'))}",
                    f"close: {_counts(f'{c}/{b}' for c, b in zip(_obs(recs, 'close_code'), _obs(recs, 'closed_by')))}"]
        return out
    if check == "tts.ws_idle_timeout":
        exp = recs[0]["expected"]
        tol = exp["tolerance_s"]
        checks = [(r["observed"].get("checks"), r["observed"]) for r in recs]
        derived = any(c is None for c, _ in checks)
        timing = sum(c["timing"] if c else (o["idle_s"] is not None and
                     exp["idle_close_s"] - tol[0] <= o["idle_s"] <= exp["idle_close_s"] + tol[1]) for c, o in checks)
        closed = sum(c["server_closed"] if c else o["closed_by"] == "server" for c, o in checks)
        tag = " (derived)" if derived else ""
        out = [f"idle before close: {_stats(_obs(recs, 'idle_s'), 's')}; expected {exp['idle_close_s']} s "
               f"(-{tol[0]}/+{tol[1]})",
               f"timing within tolerance{tag}: {timing}/{len(recs)}",
               f"server close handshake{tag}: {closed}/{len(recs)}",
               f"close: {_counts(f'{c}/{b}' for c, b in zip(_obs(recs, 'close_code'), _obs(recs, 'closed_by')))}",
               f"error frame: {_counts(_obs(recs, 'error_message'))}"]
        if "pong" in recs[0]["observed"]:
            out.append(f"pong: {_counts(_obs(recs, 'pong'))}")
        return out
    if check == "tts.ws_lifecycle":
        exp = recs[0]["expected"]
        out = [f"terminal: {_counts(_obs(recs, 'terminal'))}"]
        if "gap_ms" in exp:
            out.append(f"last chunk → complete: {_stats(_obs(recs, 'gap_ms'))}; expected {exp['gap_ms']} ms")
        out.append(f"after terminal: {_counts(f'{c}/{b}' for c, b in zip(_obs(recs, 'close_code'), _obs(recs, 'closed_by')))}"
                   f"; terminal → close {_stats(_obs(recs, 'terminal_to_close_s'), 's')}")
        if "reuse" in recs[0]["observed"]:
            out.append(f"second synthesis on same socket: {_counts(_obs(recs, 'reuse'))}")
        if recs[0]["observed"].get("error_message") or "gap_ms" not in exp:
            out.append(f"error frame: {_counts(_obs(recs, 'error_message'))}")
        return out
    if check == "tts.ttfa":
        return [f"TTFA: {_stats(_obs(recs, 'ttfa_ms'))}", f"connect: {_stats(_obs(recs, 'connect_ms'))}",
                f"connection: {_counts(_obs(recs, 'connection'))}", f"peer IP: {_counts(r.get('peer_ip') for r in recs)}"]
    if check == "tts.token_expiry":
        keys = [k for k, v in recs[0]["observed"].items()
                if k != "status" and (not isinstance(v, (dict, list)) or k == "body")]
        return [f"status: {_counts(r['status'] for r in recs)}"] + [f"{k}: {_counts(_obs(recs, k))}" for k in keys]
    return [f"status: {_counts(r['status'] for r in recs)}", f"duration: {_stats(durations)}"]


def _aggregate_table(aggs: list[dict]) -> list[str]:
    exp = aggs[0]["expected"]
    lines = ["", f"Claims: streaming p50 ≤ {exp['claim_streaming_p50_ms']} ms, benchmark p50 ≤ "
                 f"{exp['claim_benchmark_p50_ms']} ms; product SLO p95 ≤ {exp['slo_p95_ms']} ms. "
                 "Percentiles are nearest-rank (with n=10, p95 = the largest sample).", "",
             "| case | n | p50 ms | p95 ms | min–max ms | ≤ streaming claim | ≤ benchmark claim | ≤ SLO p95 | outcome |",
             "|---|---|---|---|---|---|---|---|---|"]
    for a in aggs:
        o, c = a["observed"], a["observed"]["checks"]
        lines.append(f"| {a['case']} | {o['n']} | {o['p50_ms']} | {o['p95_ms']} | {o['min_ms']}–{o['max_ms']} | "
                     f"{'yes' if c['claim_streaming'] else 'no'} | {'yes' if c['claim_benchmark'] else 'no'} | "
                     f"{'yes' if c['slo_p95'] else 'no'} | {a['outcome']} |")
    return lines


def render(headers: list[dict], records: list[dict]) -> str:
    requests = [r for r in records if r.get("record") == "request"]
    aggregates = [r for r in records if r.get("record") == "aggregate"]
    billed = sum(r.get("chars_sent", 0) for r in requests if r.get("audio_len"))
    lines = ["# Evidence summary", "",
             f"{len(headers)} runs, {len(requests)} requests, {billed} chars billed (requests that returned audio). "
             "Outcomes are as recorded by each check; \"derived\" marks values recomputed here from stored fields.", "",
             "| run | git sha | location | host | resolved IPs | started |", "|---|---|---|---|---|---|"]
    for h in headers:
        lines.append(f"| `{h['run_id']}` | `{h.get('git_sha', '')[:12]}` | {h.get('location_note', '')} | "
                     f"{h.get('vendor_host', '')} | {', '.join(h.get('vendor_ips', [])) or '-'} | "
                     f"{_ist(h['ts_ist']):%d/%m/%Y %H:%M} IST |")

    groups = defaultdict(list)
    for r in requests:
        groups[(r["check"], r.get("case") or r.get("transport"))].append(r)  # Step 2 smoke records have no case
    checks = sorted({c for c, _ in groups}, key=lambda c: ORDER.index(c) if c in ORDER else len(ORDER))
    for check in checks:
        cases = [case for c, case in groups if c == check]
        runs = sorted({r["run_id"] for (c, _), rs in groups.items() if c == check for r in rs})
        lines += ["", f"## {check}", "", "runs: " + ", ".join(f"`{r}`" for r in runs), "",
                  "| case | n | outcomes | key numbers | IST window | example IDs (server, client) |",
                  "|---|---|---|---|---|---|"]
        for case in cases:
            recs = groups[(check, case)]
            outcomes = ", ".join(f"{k} {v}" for k, v in sorted(Counter(r["outcome"] for r in recs).items()))
            lines.append(f"| {case} | {len(recs)} | {outcomes} | {'<br>'.join(_numbers(check, recs))} | "
                         f"{_window(recs)} | {_example_ids(recs)} |")
        check_aggs = [a for a in aggregates if a["check"] == check]
        if check_aggs:
            lines += _aggregate_table(check_aggs)
    return mask("\n".join(lines) + "\n")


def summarize(paths: list[Path]) -> str:
    return render(*load(paths))
