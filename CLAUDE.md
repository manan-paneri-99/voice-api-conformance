# CLAUDE.md

Start of every session:
1. Read `SPEC.md` — the design, approved decisions, and numbered build steps.
2. If `pilot/BRIEF.md` exists, read it: private pilot context, hard rules, hypotheses, report format. Never copy its contents into tracked files.
3. Check "Current step" below, do only that step, then stop and report.

## Hard rules
- Never print, log or commit the API key. It lives in `.env` (gitignored); never read `.env`. Mask keys/tokens as `sk_...` everywhere.
- Max 5 concurrent requests to any vendor, enforced in code. No load, stress or security testing.
- Before any live run, state the check, the number of requests and the estimated characters, then run it.
- Docs claims are hypotheses until a live run confirms them. Never write a finding the evidence doesn't support.
- Don't write the pilot report's findings; produce evidence and numbers. The user writes the report.
- `pilot/` and `evidence/` are private (gitignored). Don't publish or push anything without asking.

## Conventions
Python 3.11+, asyncio, httpx, websockets, pyyaml, python-dotenv, argparse. Venv in `.venv` (`python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'`). Tests: `.venv/bin/pytest` (offline only).

## Current step
Step 4 (WS checks) — done: `tts.ws_idle_timeout`, `tts.ws_lifecycle` in `vac/checks/tts.py`; live runs written to `evidence/`. Next: Step 5 (latency, `tts.ttfa`).
