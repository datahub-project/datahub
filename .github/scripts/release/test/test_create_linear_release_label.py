"""Label names created for a cut release tag."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import create_linear_release_label as crl


def test_oss_release_label_uses_prefix():
    assert crl.release_label_name("v1.7.0.1", "OSS Release") == "OSS v1.7.0.1"
    assert crl.release_label_name(" v1.7.0 ", "OSS Release") == "OSS v1.7.0"


def test_other_group_keeps_the_tag():
    assert crl.release_label_name("v2.3.0-cloud", "Saas Release") == "v2.3.0-cloud"


def test_run_reuses_workspace_label(monkeypatch):
    monkeypatch.setenv("INGESTION_LINEAR_KEY", "k")
    monkeypatch.setattr(crl, "find_group_id", lambda *_a, **_k: "GRP")
    monkeypatch.setattr(crl, "find_existing_label_id", lambda *_a, **_k: None)
    monkeypatch.setattr(crl, "find_label_id_by_name", lambda *_a, **_k: "LBL")

    def fail_create(*_args, **_kwargs):
        raise AssertionError("create should not run when the name already exists")

    monkeypatch.setattr(crl, "create_label", fail_create)
    assert crl.run("v1.7.0.1", "OSS Release") == 0
