"""vac command line."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from vac.adapters.base import TTSRequest
from vac.adapters.smallest import SmallestTTS
from vac.evidence import EvidenceWriter, new_run_id
from vac.profiles import load_product, load_vendor

PROFILES = Path("profiles")
AUDIO_EXT = {"audio/wav": "wav", "audio/x-wav": "wav", "audio/mpeg": "mp3"}


def _save_audio(writer: EvidenceWriter, cap, crid: str) -> str | None:
    if not cap.audio:
        return None
    ext = "pcm" if cap.transport == "ws" else AUDIO_EXT.get(cap.headers.get("content-type", "").split(";")[0], "pcm")
    audio_dir = writer.path.with_suffix("")
    audio_dir.mkdir(exist_ok=True)
    path = audio_dir / f"{crid}.{ext}"
    path.write_bytes(cap.audio)
    return str(path)


async def smoke(args) -> int:
    vendor = load_vendor(PROFILES / "vendors" / f"{args.vendor}.yaml")
    product = load_product(PROFILES / "products" / f"{args.product}.yaml")
    api_key = os.environ.get(vendor["auth"]["env"])
    if not api_key:
        print(f"{vendor['auth']['env']} is not set (put it in .env)", file=sys.stderr)
        return 2

    text = product["inputs"]["short"]
    print(f"smoke: 3 requests (sync, sse, ws), {len(text)} chars each, {3 * len(text)} chars total")
    run_id = new_run_id()
    failed = False
    async with SmallestTTS(vendor, api_key) as adapter:
        with EvidenceWriter(run_id, known_secrets=(api_key,)) as writer:
            writer.write_header(vendor_host=adapter.host, location_note=args.location)
            for transport in ("sync", "sse", "ws"):
                crid = f"vac-{run_id}-smoke-{transport}-0"
                req = TTSRequest(text=text, voice=product["voice"], sample_rate=product["sample_rate"],
                                 model=vendor["model"], client_request_id=crid)
                if transport == "sync":
                    cap = await adapter.synth_sync(req)
                elif transport == "sse":
                    cap = await adapter.synth_sse(req)
                else:
                    async with adapter.ws_session({}) as session:
                        cap = session.capture
                        if await session.open():
                            await session.send_json(adapter.body(req))
                            await session.recv_until(
                                lambda f: isinstance(f.get("data"), dict)
                                and f["data"].get("status") in ("complete", "error"), timeout=30)
                            await session.wait_close(timeout=10)
                record = {"check": "smoke", "iteration": 0, "vendor": vendor["vendor"], "host": adapter.host,
                          "client_request_id": crid, "chars_sent": len(text), "outcome": "observed",
                          **cap.evidence(), "audio_path": _save_audio(writer, cap, crid)}
                writer.write(record)
                ok = cap.error is None and record["status"] in (101, 200) and cap.audio
                failed |= not ok
                extra = f" closed_by={cap.closed_by} code={cap.close_code}" if transport == "ws" else ""
                print(f"  {transport:4} status={record['status']} audio_bytes={len(cap.audio)} "
                      f"server_ids={writer.masked(cap.server_ids)}{extra}"
                      + (f" error={writer.masked(cap.error)}" if cap.error else ""))
    print(f"evidence: {writer.path}")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="vac")
    sub = parser.add_subparsers(dest="command", required=True)
    p_smoke = sub.add_parser("smoke", help="one short call per transport: proves auth, prints server IDs")
    p_smoke.add_argument("--vendor", required=True)
    p_smoke.add_argument("--product", default="example")
    p_smoke.add_argument("--location", default="unspecified", help="client location note for the run header")
    args = parser.parse_args(argv)
    if args.command == "smoke":
        return asyncio.run(smoke(args))
    return 2


if __name__ == "__main__":
    sys.exit(main())
