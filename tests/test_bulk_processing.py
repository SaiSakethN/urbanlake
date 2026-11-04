import gzip
import hashlib
import json

import pytest

from urbanlake.bulk_processing import (
    completed_manifest,
    month_predicate,
    parse_arguments,
    verify_page_hashes,
    writer_lock,
)


def make_manifest(root):
    folder = root / "partitions" / "2020-01"
    folder.mkdir(parents=True)
    page = folder / "page-000001-cafe.jsonl.gz"
    with gzip.open(page, "wt") as handle:
        handle.write("{}\n")
    digest = hashlib.sha256(page.read_bytes()).hexdigest()
    payload = {
        "complete": True,
        "status": "complete",
        "partition_id": "2020-01",
        "run_id": "bootstrap",
        "records": 1,
        "pages": [
            {"path": "partitions/2020-01/page-000001-cafe.jsonl.gz", "records": 1, "sha256": digest}
        ],
    }
    (folder / "state.json").write_text(json.dumps(payload))
    return folder, payload, page


def test_only_count_reconciled_month_is_eligible(tmp_path):
    folder, state, _ = make_manifest(tmp_path)
    valid, paths = completed_manifest(tmp_path, "2020-01")
    verify_page_hashes(valid, paths)
    assert len(paths) == 1
    state["complete"] = False
    (folder / "state.json").write_text(json.dumps(state))
    with pytest.raises(ValueError, match="complete"):
        completed_manifest(tmp_path, "2020-01")


def test_page_hashes_detect_archive_mutation(tmp_path):
    _, _, page = make_manifest(tmp_path)
    state, paths = completed_manifest(tmp_path, "2020-01")
    page.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SHA-256"):
        verify_page_hashes(state, paths)


def test_manifest_cannot_escape_root_or_include_partial_files(tmp_path):
    folder, state, _ = make_manifest(tmp_path)
    state["pages"][0]["path"] = "../../../outside.jsonl.gz"
    (folder / "state.json").write_text(json.dumps(state))
    with pytest.raises(ValueError, match="input root"):
        completed_manifest(tmp_path, "2020-01")


def test_partition_sql_rejects_untrusted_input():
    assert month_predicate(["2020-02", "2020-01"]) == "created_month IN ('2020-01','2020-02')"
    with pytest.raises(ValueError):
        month_predicate(["2020-01' OR true"])


def test_writer_lock_blocks_a_second_writer_and_releases(tmp_path):
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError, match="another bulk writer"):
            with writer_lock(tmp_path):
                pass
    with writer_lock(tmp_path):
        pass


def test_changed_source_count_cannot_be_finalized(tmp_path):
    folder, state, _ = make_manifest(tmp_path)
    state["source_count_final"] = 2
    (folder / "state.json").write_text(json.dumps(state))
    with pytest.raises(ValueError, match="requires reconciliation"):
        completed_manifest(tmp_path, "2020-01")


def test_combined_action_requires_one_completed_month():
    args = parse_arguments(["consume-finalize", "--partition", "2020-01"])
    assert args.action == "consume-finalize"
    assert args.partition == "2020-01"
    with pytest.raises(SystemExit):
        parse_arguments(["consume-finalize"])
    with pytest.raises(SystemExit):
        parse_arguments(["consume-finalize", "--partition", "2020-13"])


def test_original_actions_remain_compatible():
    assert parse_arguments(["consume"]).action == "consume"
    assert parse_arguments(["finalize", "--partition", "2020-01"]).action == "finalize"
    assert parse_arguments(["load-files", "--partition", "2020-01"]).action == "load-files"
