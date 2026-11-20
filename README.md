# voice-api-conformance

Run a voice API (TTS today, STT later) against two contracts:

- **Vendor contract**: what the vendor's docs promise (limits, error codes, timeouts, latency).
- **Product contract**: what your product needs (your inputs, your SLOs).

Every run is logged as JSONL evidence with request IDs and timestamps, so a finding can be checked by someone else.

## Status
TTS checks are built for one vendor adapter (Smallest AI, sync / SSE / WebSocket): smoke, concurrency, limits, error shape, WS idle timeout and lifecycle, TTFA, token expiry, and whole-conversation consistency (persistent, reconnect, parallel, multi-speaker meeting). Design and decisions: [SPEC.md](SPEC.md).

## Usage
```
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
cp .env.example .env              # add your own API key
.venv/bin/vac smoke --vendor smallest
.venv/bin/vac run --vendor smallest --check tts.ttfa
.venv/bin/vac summarize evidence/<run_id>.jsonl
.venv/bin/pytest                  # offline, against a fake server
```
Requests are capped at 5 in flight in code, and each run stops if its estimated characters exceed the product profile's budget. This is a conformance tool, not a load tester.

## Future scope
- STT analysis: realtime WebSocket and pre-recorded adapters; time from end of speech to final transcript, end-of-turn behaviour, keep-alive and idle timeout, error shape.
- STT → LLM → TTS loop: voice-to-voice latency per conversation turn.
- More TTS cases: barge-in (cancelling mid-stream), streaming LLM text in, telephony formats (8 kHz μ-law), a second vendor adapter, CI mode with exit codes against SLOs.
