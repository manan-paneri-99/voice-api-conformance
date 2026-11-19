# SPEC: voice-api-conformance

## Problem

A team evaluating a voice API vendor during a pilot (say one week) needs to know two things:

1. **Does the API behave as its docs say?** Limits, error codes and shapes, timeouts, streaming lifecycle, latency claims.
2. **Does it meet our product's needs?** Our real inputs, our transports, our SLOs.

Most teams check this by hand once and then forget it. This tool turns both into repeatable checks. Each run leaves evidence (request IDs, timestamps, numbers from several runs) that a vendor or a reviewer can verify.

## Principles

- **Vendor-neutral.** Checks are written against an adapter interface. A vendor is one adapter plus one vendor profile.
- **Product-neutral.** A product profile supplies inputs and SLOs. Checks never hard-code product values.
- **Evidence first.** Every request is logged with the client request ID, server request IDs, IST and UTC timestamps, and timings. Pass/fail without evidence is not a result.
- **Polite by construction.** Concurrency is hard-capped at 5 in code, and no config can raise it. This is not a load-testing or security tool.
- **Engineering, not ML.** Checks judge contracts, failure behaviour and timing, never voice quality.

## v1 scope

- In: TTS over sync HTTP, SSE and WebSocket. One adapter: Smallest (Waves/Models API, `lightning_v3.1_pro`).
- Out: STT (designed for in the interface, not built), other vendors, UI, CI.

## Layout

```
vac/
  adapters/base.py      # interface + raw capture types
  adapters/smallest.py  # Smallest TTS adapter
  evidence.py           # JSONL writer, key masking, timestamps
  runner.py             # runs checks, n runs each, concurrency cap 5, credit budget guard
  checks/tts.py         # check implementations
  profiles.py           # load + validate vendor/product YAML
  summarize.py          # evidence JSONL -> per-check numbers (markdown table)
  cli.py                # `vac run`, `vac summarize`, `vac smoke`
profiles/vendors/smallest.yaml
profiles/products/example.yaml
tests/                  # offline unit tests only, no network
evidence/               # gitignored, run output
pilot/                  # gitignored, private pilot notes and docs snapshot
```

Dependencies (approved): httpx, websockets, pyyaml, python-dotenv; pytest and pytest-asyncio for dev. CLI uses stdlib argparse. Add nothing else without asking.

## Adapter interface

The adapter captures **raw** behaviour and doesn't interpret it. Checks do the interpreting.

```python
class TTSAdapter(Protocol):
    name: str
    async def synth_sync(self, req: TTSRequest) -> HttpCapture: ...
    async def synth_sse(self, req: TTSRequest) -> StreamCapture: ...
    async def ws_session(self, url_params: dict) -> WSSession: ...   # open/send/recv/close, every frame captured
```

- `TTSRequest`: text, voice, sample_rate, speed, model, client_request_id, extra (vendor-specific params, passed through untouched).
- `HttpCapture`: status, headers, body (bytes plus parsed JSON if parseable), timings `{t_send, t_headers, t_first_byte, t_end}` (perf_counter, ms).
- `StreamCapture`: the same, plus `frames: [{t_ms, event, data}]`.
- `WSSession` capture: handshake status and headers, every frame `{t_ms, direction, kind(text|binary), data or len}`, close code and reason, who closed first.
- Each capture also stores the server request IDs it found, from headers or frames. The vendor profile says which field names to look for.

Timing definitions (state them in the report, don't change them silently):
- **TTFA (time to first audio)**: from sending the request (HTTP) or text frame (WS, on an already open socket) to the first frame carrying audio.
- **Connect time** is recorded separately, never folded into TTFA.
- Record whether the connection was cold or warm.

## Profiles

`profiles/vendors/<vendor>.yaml` holds documented claims. Each claim has an id, a value and a source URL:

```yaml
vendor: smallest
base_url: https://api.smallest.ai/waves/v1
auth: {header: Authorization, scheme: Bearer, env: SMALLEST_API_KEY}
request_id_fields: {headers: [x-request-id, X-External-Request-Id], frames: [request_id, session_id]}
claims:
  tts.concurrency.max_active: {value: 1, on_exceed: 429, source: https://docs.smallest.ai/api-reference/concurrency-and-limits}
  tts.text.max_chars:        {value: 8000, source: https://docs.smallest.ai/models/api-reference/text-to-speech/synthesize-speech-sse}
  tts.ws.idle_timeout_s:     {default: 60, param: timeout, max: 180, source: https://docs.smallest.ai/models/api-reference/text-to-speech/tts}
  tts.ws.complete_backoff_ms:{default: 4000, max: 10000, source: https://docs.smallest.ai/models/api-reference/text-to-speech/tts}
  tts.ttfa_ms:               {value: 100, note: "streaming page says ~100ms; overview says ~200ms TTFB", source: https://docs.smallest.ai/models/text-to-speech/streaming}
```

`profiles/products/<product>.yaml` holds the product's inputs and SLOs:

```yaml
product: example
voice: <voice_id>
sample_rate: 24000
transport: ws
inputs:
  short: "Your order has shipped."
  typical: "<~200 chars, real product sentence>"
slo:
  ttfa_p95_ms: 300
  success_rate: 0.999
budget: {max_chars_total: 200000}
```

## Evidence record (one JSON line per request)

```json
{"run_id": "...", "check": "tts.concurrency", "iteration": 3, "vendor": "smallest",
 "transport": "sync", "client_request_id": "vac-...", "server_ids": {"x-request-id": "..."},
 "ts_utc": "...Z", "ts_ist": "...+05:30", "host": "api.smallest.ai",
 "status": 429, "headers_subset": {...}, "body_excerpt": "...", "timings_ms": {...},
 "chars_sent": 42, "outcome": "observed", "notes": ""}
```

- Mask anything that looks like a key or token to `sk_...` before writing. Tests must cover this.
- Write a run header record: tool version, git sha, client location note, vendor host.

## Checks (v1, TTS)

Each check reads its expected value from the vendor profile (and the product profile where relevant), runs n times, and records **expected vs observed** per iteration. Default n=5. TTFA uses n=10.

| id | Procedure | Expected (from profile) | Records |
|---|---|---|---|
| `tts.concurrency` | 2 sync requests released together (barrier), 5 rounds | 2nd gets 429; note `Retry-After` presence/value | statuses, headers, overlap timing |
| `tts.limits` | 8000 vs 8001 chars; whitespace-only; speed 0.4/2.1; sample_rate 22050 | over-limit rejected with documented code; at-limit accepted | status, body shape |
| `tts.error_shape` | same invalid param over sync, SSE, WS | same error semantics across transports | body/frame shape per transport |
| `tts.ws_idle_timeout` | open WS with `timeout=5`, then `timeout=200`; send nothing | closes at ~5 s; 200 clamped to 180 | time to close, close code, last frame |
| `tts.ws_lifecycle` | one synthesis on WS; vary `complete_backoff_ms` | `complete` is terminal, server closes after; backoff adds ~configured delay | frame timeline, close code, who closed |
| `tts.ttfa` | n=10 per transport (SSE, WS), product `typical` input | vendor claim and product SLO, both reported | TTFA p50/p95, connect time, cold/warm |
| `tts.token_expiry` (optional) | short-lived token (TTL 30 s) on an open WS | documented behaviour on expiry | frames, close code |
| `tts.conversation` | scripted multi-turn conversation (product profile); cases `persistent`, `reconnect`, `parallel`, `meeting` | product `slo.conversation`, every turn and the whole conversation | per-turn timings, chunk timeline, playback model, IDs |

Guards: runner refuses to start if the estimated chars exceed the remaining `budget.max_chars_total`. Checks run sequentially. Only `tts.concurrency` runs requests in parallel (2 at once).

## CLI

```
vac smoke  --vendor smallest                      # 1 short sync call: proves auth, prints server IDs (masked key)
vac run    --vendor smallest --product example --check tts.concurrency [--runs 5]
vac run    --vendor smallest --product example --all
vac summarize evidence/<run_id>.jsonl             # markdown: per check n, pass/fail counts, p50/p95, IDs, IST window
```

## Build steps (one slice each; stop after each)

1. **Skeleton + evidence.** `evidence.py` (JSONL, masking, IST/UTC), `profiles.py` (load + validate), the two YAML profiles, offline tests. Verify: `pytest`.
2. **Smallest adapter + smoke.** `adapters/base.py`, `adapters/smallest.py` (sync, SSE, WS), `vac smoke`. Verify: one live smoke call; show status and server IDs.
3. **Runner + first checks.** `runner.py` (cap 5, budget guard), `tts.concurrency`, `tts.limits`, `tts.error_shape`. Verify: live runs, evidence file written.
4. **WS checks.** `tts.ws_idle_timeout`, `tts.ws_lifecycle`.
5. **Latency.** `tts.ttfa`.
6. **Summarize.** `summarize.py`. Verify against step 3–5 evidence.
7. Optional: `tts.token_expiry`.
8. **Conversation consistency.** `tts.conversation`, product `conversation`/`meeting` scripts and `slo.conversation`, summarize table. See below.

## Step 8: conversation consistency

A voice agent holds one conversation, not isolated requests. The product SLOs must hold on **every turn** and across the
**whole conversation**, first turn to last. Not part of `--all` (about 30 min wall time).

Script (product profile): agent turns with text and `pause_s`, the user's speaking/thinking time before the turn. One
pause is longer than the idle timeout. Same script every run, so runs are comparable.

Turn timing (per turn):
- **turn send** = max(previous turn's playback end + `pause_s`, previous turn's `complete` frame). `blocked_by_complete_ms`
  records how long waiting for `complete` (its backoff) delayed the turn.
- **TTFA** as defined above (from the text frame). **turn start** = connect time (if this turn opened a socket) + TTFA:
  what the user waits for. The turn-start SLOs judge turn start.
- **max gap**: longest gap between consecutive audio chunks.
- **playback model**: PCM 16-bit mono, duration = bytes / (2 × sample_rate). Playback starts at first chunk + `jitter_buffer_ms`;
  a chunk that arrives after the playhead needs it is an **underrun** (the playhead waits for it, then continues).
- success = audio received and terminal frame `complete`.

Per conversation (aggregate record): turn start p50/p95/max, TTFA p50/p95, max gap, underruns, drift (median turn start of
the last third minus the first third), spread (max − p50), failed turns, reconnects, blocked_by_complete total; each
`slo.conversation` value judged separately, outcome `match` only if all hold.

Cases:
- `persistent`: one socket for the conversation, `{"type":"ping"}` every `keepalive_every_s` during waits; if the socket
  drops, the next turn reopens it (counted as a reconnect).
- `reconnect`: a new socket per turn (connect time counted in turn start).
- `parallel`: 3 persistent conversations at once, starts staggered; at most 3 sockets.
- `meeting`: 4 speakers (one voice and one socket each) taking quick turns; a turn with `overlap: true` starts together
  with the previous turn. At most 4 sockets.

## Later (not v1)

STT adapter (WS + pre-recorded), a second vendor adapter, an STT→LLM→TTS loop latency check, CI mode with exit codes against SLOs.
