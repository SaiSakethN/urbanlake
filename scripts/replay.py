"""Replay saved observations without changing observation IDs or observed_at."""

import argparse

from urbanlake.contracts import Observation
from urbanlake.messaging import Publisher

parser = argparse.ArgumentParser()
parser.add_argument("paths", nargs="+")
args = parser.parse_args()
publisher = Publisher()
count = 0
for path in args.paths:
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            publisher.send(Observation.model_validate_json(line))
            count += 1
publisher.flush()
print(f"Kafka acknowledged {count} observations")
