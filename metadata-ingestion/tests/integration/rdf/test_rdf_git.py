import os
import pathlib
from unittest.mock import patch

import git
import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.rdf.ingestion.rdf_source import RDFSourceReport

_SKOS_TTL = """\
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/glossary/> .

ex:ConceptA a skos:Concept ;
    skos:prefLabel "Concept A" ;
    skos:definition "First concept." .

ex:ConceptB a skos:Concept ;
    skos:prefLabel "Concept B" ;
    skos:definition "Second concept." .
"""


@pytest.mark.integration
def test_git_clone_and_walk(
    tmp_path: pathlib.Path, mock_datahub_graph_instance: DataHubGraph
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    repo = git.Repo.init(repo_dir)
    with repo.config_writer() as cw:
        cw.set_value("user", "email", "test@example.com")
        cw.set_value("user", "name", "Test")
    glossary_dir = repo_dir / "glossary"
    glossary_dir.mkdir()
    (glossary_dir / "concepts.ttl").write_text(_SKOS_TTL, encoding="utf-8")
    repo.index.add(["glossary/concepts.ttl"])
    repo.index.commit("add glossary")

    output_path = tmp_path / "output.json"
    pipeline = Pipeline.create(
        {
            "run_id": "test-rdf-git",
            "source": {
                "type": "rdf",
                "config": {
                    "source": "glossary",
                    "format": "turtle",
                    "git_info": {
                        "repo": "https://github.com/acme/onto",
                        "repo_ssh_locator": f"file://{repo_dir}",
                    },
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    report = pipeline.source.get_report()
    assert isinstance(report, RDFSourceReport)
    assert report.git_checkout is not None
    assert report.num_workunits_produced > 0
    assert report.num_glossary_terms == 2


@pytest.mark.integration
def test_clone_failure_reports_gracefully(
    tmp_path: pathlib.Path, mock_datahub_graph_instance: DataHubGraph
) -> None:
    output_path = tmp_path / "output.json"
    with patch(
        "datahub.configuration.git.GitInfo.clone",
        side_effect=RuntimeError("boom"),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "test-rdf-git-fail",
                "source": {
                    "type": "rdf",
                    "config": {
                        "git_info": {
                            "repo": "https://github.com/acme/onto",
                            "repo_ssh_locator": "git@github.com:acme/onto.git",
                        },
                    },
                },
                "sink": {"type": "file", "config": {"filename": str(output_path)}},
            }
        )
        pipeline.ctx.graph = mock_datahub_graph_instance
        pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, RDFSourceReport)
    assert report.failures
    assert report.num_workunits_produced == 0


@pytest.mark.integration
def test_symlink_escaping_checkout_is_not_loaded(
    tmp_path: pathlib.Path, mock_datahub_graph_instance: DataHubGraph
) -> None:
    # A file outside the repo that must never be ingested via the checkout.
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    (outside_dir / "secret.ttl").write_text(_SKOS_TTL, encoding="utf-8")

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    repo = git.Repo.init(repo_dir)
    with repo.config_writer() as cw:
        cw.set_value("user", "email", "test@example.com")
        cw.set_value("user", "name", "Test")
    # A symlink whose textual path is a clean relative name but resolves outside.
    os.symlink(outside_dir / "secret.ttl", repo_dir / "escape.ttl")
    repo.index.add(["escape.ttl"])
    repo.index.commit("add escaping symlink")

    output_path = tmp_path / "output.json"
    pipeline = Pipeline.create(
        {
            "run_id": "test-rdf-git-symlink",
            "source": {
                "type": "rdf",
                "config": {
                    "source": "escape.ttl",
                    "format": "turtle",
                    "git_info": {
                        "repo": "https://github.com/acme/onto",
                        "repo_ssh_locator": f"file://{repo_dir}",
                    },
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()

    report = pipeline.source.get_report()
    assert isinstance(report, RDFSourceReport)
    assert report.git_checkout is not None
    # Containment is enforced against the checkout, so the outside file is
    # rejected and nothing is emitted.
    assert report.failures
    assert report.num_glossary_terms == 0
    assert report.num_workunits_produced == 0
