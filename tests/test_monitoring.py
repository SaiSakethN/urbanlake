import json

from urbanlake.monitoring import PipelineCollector


def test_pipeline_evidence_not_invented(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    missing = list(PipelineCollector().collect())
    assert missing[0].samples[0].value == 0
    (tmp_path / "artifacts").mkdir()
    (tmp_path / "artifacts/pipeline_metrics.json").write_text(json.dumps({"silver_requests": 17}))
    available = list(PipelineCollector().collect())
    assert available[0].samples[0].value == 1
    assert available[1].samples[0].value == 17
