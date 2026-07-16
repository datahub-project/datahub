from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.abs.source import ABSSource


def _abs_source(include: str) -> ABSSource:
    return ABSSource.create(
        {
            "path_specs": [{"include": include, "emit_folders_only": True}],
            "azure_config": {
                "account_name": "acct",
                "container_name": "media",
                "sas_token": "dummy",
            },
        },
        PipelineContext(run_id="abs-folders-only-test"),
    )


def test_abs_emit_folders_only_emits_containers():
    source = _abs_source("https://acct.blob.core.windows.net/media/videos/*/")

    # resolve_templated_folders yields container-relative folder paths.
    with patch.object(
        source,
        "resolve_templated_folders",
        return_value=iter(["videos/2023/", "videos/2024/"]),
    ):
        wus = list(source.get_workunits_internal())

    urns = {wu.get_urn() for wu in wus}
    assert urns  # non-empty
    assert all(u.startswith("urn:li:container:") for u in urns)
    assert not any(u.startswith("urn:li:dataset:") for u in urns)
    assert source.report.folders_scanned == 2
