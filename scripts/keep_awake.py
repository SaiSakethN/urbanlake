"""Temporary Windows execution-state request for the active full-data job."""

import argparse
import ctypes
import json
import os
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--max-hours", type=float, default=12)
args = parser.parse_args()
deadline = time.monotonic() + args.max_hours * 3600
continuous, system_required = 0x80000000, 0x00000001
windows = os.name == "nt"
try:
    if windows:
        if not ctypes.windll.kernel32.SetThreadExecutionState(continuous | system_required):
            raise OSError("Windows execution-state request failed")
    print("Temporary system-sleep prevention active for full-data processing", flush=True)
    while time.monotonic() < deadline:
        status = json.loads(Path("artifacts/full-project-status.json").read_text())
        if status.get("status") != "running":
            break
        pipeline = Path("data/full/backfill/pipeline.json")
        if pipeline.exists() and time.time() - pipeline.stat().st_mtime > 1800:
            break
        time.sleep(30)
finally:
    if windows:
        ctypes.windll.kernel32.SetThreadExecutionState(continuous)
    print("Temporary execution-state request released", flush=True)
