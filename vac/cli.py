"""vac command line."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from vac.adapters.smallest import SmallestTTS
from vac.checks.tts import ALL, CHECKS
from vac.profiles import load_product, load_vendor
from vac.runner import BudgetExceeded, run_checks
from vac.summarize import summarize

PROFILES = Path("profiles")


async def _run(args, check_ids: list[str], runs: int | None) -> int:
    vendor = load_vendor(PROFILES / "vendors" / f"{args.vendor}.yaml")
    product = load_product(PROFILES / "products" / f"{args.product}.yaml")
    api_key = os.environ.get(vendor["auth"]["env"])
    if not api_key:
        print(f"{vendor['auth']['env']} is not set (put it in .env)", file=sys.stderr)
        return 2
    async with SmallestTTS(vendor, api_key) as adapter:
        try:
            await run_checks(adapter, vendor, product, check_ids, runs, args.location, (api_key,))
        except BudgetExceeded as exc:
            print(f"refusing to run: {exc}", file=sys.stderr)
            return 3
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="vac")
    sub = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--vendor", required=True)
    common.add_argument("--product", default="example")
    common.add_argument("--location", default="unspecified", help="client location note for the run header")
    sub.add_parser("smoke", parents=[common], help="one short call per transport: proves auth, prints server IDs")
    p_run = sub.add_parser("run", parents=[common], help="run checks against vendor and product profiles")
    which = p_run.add_mutually_exclusive_group(required=True)
    which.add_argument("--check", action="append", choices=sorted(set(CHECKS) - {"smoke"}))
    which.add_argument("--all", action="store_true", help=f"run {', '.join(ALL)}")
    p_run.add_argument("--runs", type=int, help="iterations per check (default per check)")
    p_sum = sub.add_parser("summarize", help="evidence JSONL -> markdown tables (no network)")
    p_sum.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args(argv)
    if args.command == "summarize":
        print(summarize(args.files), end="")
        return 0
    if args.command == "smoke":
        return asyncio.run(_run(args, ["smoke"], 1))
    if args.runs is not None and args.runs < 1:
        parser.error("--runs must be >= 1")
    return asyncio.run(_run(args, list(ALL) if args.all else args.check, args.runs))


if __name__ == "__main__":
    sys.exit(main())
