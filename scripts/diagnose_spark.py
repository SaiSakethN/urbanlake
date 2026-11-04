"""Read-only Spark REST diagnostics from inside the active Spark container."""

import json
import urllib.request


def read(path):
    with urllib.request.urlopen(f"http://localhost:4040/api/v1/{path}", timeout=5) as response:
        return json.load(response)


try:
    applications = read("applications")
    stages = read(f"applications/{applications[0]['id']}/stages")
    fields = [
        "stageId",
        "name",
        "status",
        "numTasks",
        "executorRunTime",
        "inputBytes",
        "outputBytes",
        "shuffleReadBytes",
        "shuffleWriteBytes",
        "diskBytesSpilled",
    ]
    slowest = sorted(stages, key=lambda stage: stage.get("executorRunTime", 0), reverse=True)[:8]
    print(
        json.dumps([{field: stage.get(field) for field in fields} for stage in slowest], indent=2)
    )
except (OSError, IndexError, KeyError) as error:
    print(json.dumps({"status": "Spark UI not currently available", "detail": str(error)}))
