"""Small explicit commands; no unbounded historical backfill defaults."""

import argparse
import json
import logging
from datetime import UTC, date, datetime
from itertools import islice
from pathlib import Path
from uuid import uuid4

from urbanlake.contracts import Observation
from urbanlake.source import Socrata

log = logging.getLogger("urbanlake")


def ingest(args):
    from urbanlake.messaging import Publisher

    run_id = args.run_id or str(uuid4())
    observed_at = datetime.now(UTC)
    publisher = None if args.output else Publisher()
    handle = None
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        handle = open(args.output, "a", encoding="utf-8")
    count = 0
    try:
        with Socrata() as source:
            rows = (
                source.daily_demand(args.start, args.end)
                if args.kind == "daily_demand"
                else source.requests(
                    args.start,
                    args.end,
                    max_records=args.max_records,
                    page_size=min(1000, args.sample_limit or 1000),
                )
            )
            if args.sample_limit:
                if args.kind != "request":
                    raise ValueError("sample-limit is only supported for request observations")
                rows = islice(rows, args.sample_limit)
            for row in rows:
                observation = Observation.wrap(
                    row, kind=args.kind, run_id=run_id, observed_at=observed_at
                )
                if handle:
                    handle.write(observation.model_dump_json() + "\n")
                else:
                    publisher.send(observation)
                count += 1
        if publisher:
            publisher.flush()
    finally:
        if handle:
            handle.close()
    # Written only after all rows and all acknowledgments succeed.
    manifest = {
        "run_id": run_id,
        "kind": args.kind,
        "start": str(args.start),
        "end": str(args.end),
        "records": count,
        "complete": not bool(args.sample_limit),
        "coverage": "bounded_sample" if args.sample_limit else "full_window",
        "observed_at": observed_at.isoformat(),
    }
    Path("artifacts/manifests").mkdir(parents=True, exist_ok=True)
    Path(f"artifacts/manifests/{run_id}.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="urbanlake")
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("ingest")
    command.add_argument("--kind", choices=["request", "daily_demand"], default="request")
    command.add_argument("--start", type=date.fromisoformat, required=True)
    command.add_argument("--end", type=date.fromisoformat, required=True)
    command.add_argument("--max-records", type=int, default=10000)
    command.add_argument(
        "--sample-limit", type=int, help="Explicit partial-window sample (not full coverage)"
    )
    command.add_argument("--run-id")
    command.add_argument("--output", help="Optional JSONL observations for offline Spark replay")
    command.set_defaults(func=ingest)
    command = sub.add_parser("process")
    command.add_argument("--input", help="JSONL input; default reads Kafka with availableNow")
    command.add_argument("--lake", default="data")
    command.add_argument("--checkpoint", default="data/checkpoints/observations-v1")
    command.add_argument("--continuous", action="store_true")
    command.set_defaults(func=lambda a: __import__("urbanlake.processing", fromlist=["run"]).run(a))
    command = sub.add_parser("train")
    command.add_argument("--input", default="data/gold/demand_export")
    command.add_argument("--output", default="artifacts/model.joblib")
    command.set_defaults(func=lambda a: __import__("urbanlake.ml", fromlist=["train"]).train(a))
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
