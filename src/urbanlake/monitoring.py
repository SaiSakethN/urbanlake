"""Export actual pipeline progress and an explicit missing-evidence signal."""

import json
from pathlib import Path

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily


class PipelineCollector:
    def collect(self):
        path = Path("artifacts/pipeline_metrics.json")
        available = GaugeMetricFamily(
            "urbanlake_pipeline_metrics_available", "Pipeline evidence exists"
        )
        available.add_metric([], 1 if path.exists() else 0)
        yield available
        if path.exists():
            for key, value in json.loads(path.read_text()).items():
                metric = GaugeMetricFamily(f"urbanlake_pipeline_{key}", f"Last successful {key}")
                metric.add_metric([], float(value))
                yield metric


def main():
    import threading

    REGISTRY.register(PipelineCollector())
    start_http_server(9108)
    threading.Event().wait()


if __name__ == "__main__":
    main()
