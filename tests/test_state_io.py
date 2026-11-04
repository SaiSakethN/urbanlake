import errno
import json
from pathlib import Path

import pytest

from urbanlake import state_io


def test_sharing_violations_retry_without_exposing_partial_state(monkeypatch, tmp_path):
    path = tmp_path / "pipeline.json"
    path.write_text('{"complete":false}')
    real_replace = state_io.replace
    observed = []
    delays = []

    def temporarily_locked(source, destination):
        # Every blocked swap leaves the prior valid JSON visible to readers.
        observed.append(json.loads(Path(destination).read_text()))
        assert json.loads(Path(source).read_text()) == {"complete": True, "records": 195231}
        if len(observed) < 3:
            raise PermissionError(errno.EACCES, "Windows destination is open for reading")
        return real_replace(source, destination)

    monkeypatch.setattr(state_io, "replace", temporarily_locked)
    monkeypatch.setattr(state_io, "sleep", delays.append)
    state_io.atomic_json(path, {"complete": True, "records": 195231})
    assert observed == [{"complete": False}] * 3
    assert delays == [0.025, 0.05]
    assert json.loads(path.read_text()) == {"complete": True, "records": 195231}
    assert not list(tmp_path.glob("*.part"))


def test_permanent_permission_failure_is_bounded_and_preserves_state(monkeypatch, tmp_path):
    path = tmp_path / "pipeline.json"
    original = '{"complete":false,"records":9}'
    path.write_text(original)
    calls = []

    def permanently_locked(source, destination):
        calls.append(source)
        raise PermissionError(errno.EACCES, "permanent denial")

    monkeypatch.setattr(state_io, "replace", permanently_locked)
    monkeypatch.setattr(state_io, "sleep", lambda _: None)
    with pytest.raises(PermissionError, match="permanent denial"):
        state_io.atomic_json(path, {"complete": True}, attempts=3)
    assert len(calls) == 3
    assert path.read_text() == original
    assert not list(tmp_path.glob("*.part"))


def test_nonsharing_io_error_is_not_retried(monkeypatch, tmp_path):
    calls = []

    def disk_error(*_):
        calls.append(1)
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr(state_io, "replace", disk_error)
    with pytest.raises(OSError, match="disk full"):
        state_io.atomic_json(tmp_path / "state.json", {"records": 1})
    assert len(calls) == 1


def test_atomic_writers_use_unique_same_directory_temporaries(monkeypatch, tmp_path):
    real_replace = state_io.replace
    sources = []

    def capture(source, destination):
        sources.append(source)
        assert source.parent == destination.parent
        return real_replace(source, destination)

    monkeypatch.setattr(state_io, "replace", capture)
    path = tmp_path / "nested/state.json"
    state_io.atomic_json(path, {"records": 1})
    state_io.atomic_json(path, {"records": 2})
    assert sources[0] != sources[1]
    assert json.loads(path.read_text()) == {"records": 2}
