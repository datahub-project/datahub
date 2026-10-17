from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DashboardUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)
from tests.unit.sigma.probe_tenant import recipe, register_tenant

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test registered would otherwise redact any
# of this fixture's identifiers that contain it.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_PERSONAL_WORKSPACE_WITHHELD = (
    "1 personal space(s) seen and not listed: ingestion does not emit them "
    "under this recipe, and a personal space ('My documents') holds one "
    "person's own documents"
)
_PERSONAL_WORKBOOK_WITHHELD = (
    "1 workbook(s) in a personal space seen and not listed: ingestion does not "
    "emit them under this recipe, and a personal space ('My documents') holds "
    "one person's own documents"
)
_PERSONAL_DATA_MODEL_WITHHELD = (
    "1 Data Model(s) in a personal space seen and not listed: ingestion does "
    "not emit them under this recipe, and a personal space ('My documents') "
    "holds one person's own documents"
)
_DATA_MODELS_OFF = "ingest_data_models is false, so ingestion emits none of these"


def _workbook_ids(index: EmittedIndex) -> Set[str]:
    return {
        DashboardUrn.from_string(urn).dashboard_id
        for urn in index.urns("dashboard", with_aspect=SubTypesClass)
        if any(
            isinstance(aspect, SubTypesClass)
            and BIContainerSubTypes.SIGMA_WORKBOOK in aspect.typeNames
            for aspect in index.aspects[urn]
        )
    }


def _listings(
    *,
    workspaces_accept: tuple = (),
    workbooks_accept: tuple = (),
    data_models_accept: tuple = (),
    data_models_off: bool = False,
) -> List[ParityListing]:
    return [
        ParityListing(
            "workspaces",
            "workspaces",
            lambda index: index.container_names(BIContainerSubTypes.SIGMA_WORKSPACE),
            accept_warnings=workspaces_accept,
        ),
        ParityListing(
            "workbooks",
            "workbooks",
            _workbook_ids,
            identity=lambda record: record.attributes["id"],
            accept_warnings=workbooks_accept,
        ),
        ParityListing(
            "data_models",
            "data_models",
            lambda index: index.container_names(BIContainerSubTypes.SIGMA_DATA_MODEL),
            accept_warnings=data_models_accept,
            expect_empty=data_models_off,
        ),
    ]


def _parity(
    requests_mock: Any, tmp_path: Path, listings: List[ParityListing], **config: Any
) -> Dict[str, Dict[str, Any]]:
    register_tenant(requests_mock)
    report = assert_probe_parity(
        "sigma", recipe(**config), pipeline_ingestion("sigma", tmp_path), listings
    )
    return {label: dict(report.excluded_by(label)) for label in report.kinds}


def test_default_recipe_matches_ingestion(requests_mock: Any, tmp_path: Path) -> None:
    excluded = _parity(requests_mock, tmp_path, _listings())
    assert excluded["workspaces"] == {}
    # A workbook in a workspace that refuses the lookup is a shared entity,
    # and one /files does not list cannot be placed at all.
    assert excluded["workbooks"] == {
        "22222222-0000-0000-0000-000000000004": "ingest_shared_entities",
        "22222222-0000-0000-0000-000000000005": "missing_file_metadata",
    }
    assert excluded["data_models"] == {"Shared Model": "ingest_shared_entities"}


def test_workspace_rules_match_ingestion(requests_mock: Any, tmp_path: Path) -> None:
    excluded = _parity(
        requests_mock,
        tmp_path,
        _listings(
            workspaces_accept=(_PERSONAL_WORKSPACE_WITHHELD,),
            workbooks_accept=(_PERSONAL_WORKBOOK_WITHHELD,),
            data_models_accept=(_PERSONAL_DATA_MODEL_WITHHELD,),
        ),
        workspace_pattern={"deny": ["Finance", "My documents"]},
        ingest_shared_entities=True,
    )
    # The personal space and what is in it are withheld, not listed.
    assert excluded["workspaces"] == {"Finance": "workspace_pattern"}
    assert excluded["workbooks"] == {
        "22222222-0000-0000-0000-000000000002": "workspace_pattern",
        "22222222-0000-0000-0000-000000000005": "missing_file_metadata",
    }
    assert excluded["data_models"] == {"Finance Model": "workspace_pattern"}


def test_name_patterns_match_ingestion(requests_mock: Any, tmp_path: Path) -> None:
    excluded = _parity(
        requests_mock,
        tmp_path,
        _listings(data_models_accept=(_PERSONAL_DATA_MODEL_WITHHELD,)),
        workbook_pattern={"deny": ["Revenue.*"]},
        data_model_pattern={"allow": ["Sales.*", "Shared.*"]},
        ingest_shared_entities=True,
    )
    assert excluded["workbooks"] == {
        "22222222-0000-0000-0000-000000000001": "workbook_pattern",
        "22222222-0000-0000-0000-000000000005": "missing_file_metadata",
    }
    # "Private Model" is in the personal space, so it is withheld, not listed.
    assert excluded["data_models"] == {"Finance Model": "data_model_pattern"}


def test_data_model_switch_matches_ingestion(
    requests_mock: Any, tmp_path: Path
) -> None:
    excluded = _parity(
        requests_mock,
        tmp_path,
        _listings(
            data_models_accept=(_DATA_MODELS_OFF, _PERSONAL_DATA_MODEL_WITHHELD),
            data_models_off=True,
        ),
        ingest_data_models=False,
    )
    assert excluded["data_models"] == {
        "Sales Model": "ingest_data_models",
        "Finance Model": "ingest_data_models",
        "Shared Model": "ingest_data_models",
    }
