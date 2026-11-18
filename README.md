# voice-api-conformance

Run a voice API (TTS today, STT later) against two contracts:

- **Vendor contract**: what the vendor's docs promise (limits, error codes, timeouts, latency).
- **Product contract**: what your product needs (your inputs, your SLOs).

Every run is logged as JSONL evidence with request IDs and timestamps, so a finding can be checked by someone else.

Status: in development. See [SPEC.md](SPEC.md).
