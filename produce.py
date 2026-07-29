#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx>=0.27",
# ]
# ///
"""Send a steady stream of dummy records to arcane-push-stream.

Assumes the shared kind e2e stack is up and the ingestion service is reachable
on localhost:
    kubectl --context kind-arcane-push-stream port-forward svc/arcane-push-stream 8090:8080

Usage:
    uv run produce.py                       # 1 msg/s, forever, to /api/v1/consumer1/data
    uv run produce.py --rate 5 --count 50   # 5 msg/s, stop after 50 messages
    uv run produce.py --url http://localhost:8090 --consumer consumer1
"""

from __future__ import annotations

import argparse
import random
import signal
import string
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    _ = p.add_argument(
        "--url",
        default="http://localhost:8085",
        help="Base URL of the arcane-push-stream ingestion service (default: %(default)s).",
    )
    _ = p.add_argument(
        "--api-version",
        default="v2",
        help="API version prefix used in the ingestion route "
             "(matches settings.router.apiVersion in the ingestion helm chart; default: %(default)s).",
    )
    _ = p.add_argument(
        "--consumer",
        default="consumer1",
        help="Consumer id — must match a DataRoute CR (default: %(default)s).",
    )
    _ = p.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Messages per second (default: %(default)s).",
    )
    _ = p.add_argument(
        "--count",
        type=int,
        default=0,
        help="Stop after N messages (default: 0 = run forever).",
    )
    _ = p.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request timeout in seconds (default: %(default)s).",
    )
    _ = p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible payloads.",
    )
    return p.parse_args()


def random_message(rng: random.Random) -> str:
    words = [
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "zeta",
        "eta",
        "theta",
        "iota",
        "kappa",
        "lambda",
        "mu",
        "nu",
        "xi",
        "omicron",
        "pi",
    ]
    return " ".join(rng.sample(words, k=rng.randint(2, 5)))


def random_id(rng: random.Random) -> str:
    return "".join(rng.choices(string.digits, k=6))


def build_payload(rng: random.Random) -> dict[str, str]:
    return {
        "id": random_id(rng),
        "message": random_message(rng),
        # ISO-8601 UTC with trailing Z — same shape the arcane-e2e seed recipe used
        # and what the plugin's watermark field expects.
        "TimestampUTC": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        # Unique per-message merge key so nothing gets overwritten by the MERGE.
        "ARCANE_MERGE_KEY": uuid.uuid4().hex,
    }


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    endpoint = f"{args.url.rstrip('/')}/api/{args.api_version}/{args.consumer}/data"
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    # Ctrl-C should exit cleanly, not crash the interpreter.
    stopping = False

    def _stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    sent = 0
    errors = 0
    # Steady rate loop: sleep for whatever's left of the interval after the POST.
    with httpx.Client(timeout=args.timeout) as client:
        print(
            f"POST {endpoint} at {args.rate} msg/s"
            + (f" for {args.count} messages" if args.count else " (forever)"),
            flush=True,
        )
        while not stopping and (args.count == 0 or sent < args.count):
            payload = build_payload(rng)
            started = time.monotonic()
            try:
                r = client.post(endpoint, json=payload)
                r.raise_for_status()
                sent += 1
                print(
                    f"[{sent:>6}] {r.status_code} key={payload['ARCANE_MERGE_KEY'][:8]} "
                    f"id={payload['id']} msg={payload['message']!r}",
                    flush=True,
                )
            except httpx.HTTPError as e:
                errors += 1
                print(f"[!] error: {e}", file=sys.stderr, flush=True)

            elapsed = time.monotonic() - started
            remaining = interval - elapsed
            if remaining > 0 and not stopping:
                # Break the sleep into short slices so Ctrl-C stops promptly.
                deadline = time.monotonic() + remaining
                while not stopping and time.monotonic() < deadline:
                    time.sleep(min(0.1, deadline - time.monotonic()))

    print(f"\nsent={sent} errors={errors}", flush=True)
    return 1 if errors and sent == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
