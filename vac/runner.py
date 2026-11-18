"""Run checks n times with a credit budget guard; every request becomes one evidence record."""

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from vac.adapters.base import TTSRequest
from vac.checks.tts import CHECKS
from vac.evidence import EvidenceWriter, new_run_id

EVIDENCE = Path("evidence")
AUDIO_EXT = {"audio/wav": "wav", "audio/x-wav": "wav", "audio/mpeg": "mp3"}


class BudgetExceeded(Exception):
    pass


@dataclass
class Ctx:
    adapter: object
    vendor: dict
    product: dict
    run_id: str
    writer: EvidenceWriter

    def claim(self, claim_id: str) -> dict:
        return self.vendor["claims"][claim_id]

    def request(self, check: str, transport: str, iteration: int, text: str, case: str | None = None,
                **overrides) -> TTSRequest:
        crid = f"vac-{self.run_id}-{check}-{transport}-{iteration}" + (f"-{case}" if case else "")
        fields = {"voice": self.product["voice"], "sample_rate": self.product["sample_rate"],
                  "model": self.vendor["model"]} | overrides
        return TTSRequest(text=text, client_request_id=crid, **fields)

    def record(self, cap, check: str, iteration: int, req: TTSRequest, case: str | None = None,
               expected=None, observed=None, outcome: str = "observed", notes: str = "") -> dict:
        return {"check": check, "case": case, "iteration": iteration, "vendor": self.vendor["vendor"],
                "host": self.adapter.host, "client_request_id": req.client_request_id,
                "chars_sent": len(req.text), **cap.evidence(), "audio_path": self._save_audio(cap, req),
                "expected": expected, "observed": observed, "outcome": outcome, "notes": notes}

    def _save_audio(self, cap, req: TTSRequest) -> str | None:
        if not cap.audio:
            return None
        ext = "pcm" if cap.transport == "ws" else AUDIO_EXT.get(cap.headers.get("content-type", "").split(";")[0], "pcm")
        audio_dir = self.writer.path.with_suffix("")
        audio_dir.mkdir(exist_ok=True)
        path = audio_dir / f"{req.client_request_id}.{ext}"
        path.write_bytes(cap.audio)
        return str(path)


def spent_chars(evidence_dir: Path = EVIDENCE) -> int:
    """Characters already billed: chars_sent of every request record that returned audio."""
    total = 0
    for path in sorted(evidence_dir.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            if rec.get("record") == "request" and rec.get("audio_len"):
                total += rec.get("chars_sent", 0)
    return total


async def run_checks(adapter, vendor: dict, product: dict, check_ids: list[str], runs: int | None,
                     location: str, known_secrets: tuple[str, ...], evidence_dir: Path = EVIDENCE) -> Path:
    checks = [CHECKS[c] for c in check_ids]
    plan = [(c, runs or c.default_runs, *c.estimate(vendor, product, runs or c.default_runs)) for c in checks]
    remaining = product["budget"]["max_chars_total"] - spent_chars(evidence_dir)
    total_chars = sum(p[3] for p in plan)
    for check, n, requests, chars in plan:
        print(f"plan: {check.id}  runs={n}  requests={requests}  est_chars={chars}")
    print(f"plan total: requests={sum(p[2] for p in plan)}  est_chars={total_chars}  budget_remaining={remaining}")
    if total_chars > remaining:
        raise BudgetExceeded(f"estimated {total_chars} chars exceeds remaining budget {remaining}")

    run_id = new_run_id()
    with EvidenceWriter(run_id, evidence_dir, known_secrets) as writer:
        writer.write_header(vendor_host=adapter.host, location_note=location)
        ctx = Ctx(adapter, vendor, product, run_id, writer)
        for check, n, _, _ in plan:
            outcomes = Counter()
            for i in range(n):
                for rec in await check.run(ctx, i):
                    writer.write(rec)
                    outcomes[rec["outcome"]] += 1
                    ids = {k: v for k, v in rec["server_ids"].items() if v}
                    print(writer.masked(f"  {rec['case']} i={i} status={rec['status']} outcome={rec['outcome']} "
                                        f"observed={rec['observed']} ids={ids}"
                                        + (f" error={rec['error']}" if rec["error"] else "")))
            print(f"{check.id}: " + ", ".join(f"{k}={v}" for k, v in sorted(outcomes.items())) + " (per request)")
    print(f"evidence: {writer.path}")
    return writer.path
