"""Verify the served live dashboard and fresh authoritative snapshots."""

import json
import time

import httpx

with httpx.Client(timeout=15) as client:
    page = client.get("http://localhost:8000/progress")
    page.raise_for_status()
    assert "Overall estimate" in page.text
    assert 'fetch("/v1/progress"' in page.text
    assert "setTimeout(poll" in page.text
    first = client.get("http://localhost:8000/v1/progress")
    first.raise_for_status()
    assert first.headers["cache-control"] == "no-store"
    before = first.json()
    time.sleep(3)
    second = client.get("http://localhost:8000/v1/progress")
    second.raise_for_status()
    after = second.json()
    assert before["snapshot_at"] != after["snapshot_at"]
    assert after["downloaded_records"] >= before["downloaded_records"]
    assert after["total_records"] == 23008706
    assert len(after["months"]) == 83
    assert 0 <= after["overall_percent"] < 100 or after["status"] == "complete"
    print(
        json.dumps(
            {
                "page": "http://localhost:8000/progress",
                "status": "passed",
                "fresh_snapshots": True,
                "months": after["completed_months"],
                "downloaded": after["downloaded_records"],
                "processed": after["processed_records"],
                "overall_percent": after["overall_percent"],
            },
            indent=2,
        )
    )
