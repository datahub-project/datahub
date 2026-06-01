"""Validation tests for the typed Dremio API response models.

Focused on the two contracts that are easy to regress without a test:

* `DremioDatasetResponse` allows null `LOCATION_ID` because Community-edition
  servers omit it for some views — the legacy dict path defaulted it to "",
  this is the pydantic equivalent.
* `DremioContainerResponse.extract_name_from_path_if_missing` refuses to
  fabricate an "unknown" entity when both `name` and `path` are missing — a
  silent fabrication there would corrupt downstream URNs.
"""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dremio.dremio_models import (
    DremioContainerResponse,
    DremioDatasetResponse,
)


class TestDremioDatasetResponseValidation:
    def _minimum_payload(self, **overrides):
        payload = {
            "RESOURCE_ID": "rid-1",
            "TABLE_NAME": "tbl",
            "TABLE_SCHEMA": '[s, "tbl"]',
            "LOCATION_ID": "loc-1",
        }
        payload.update(overrides)
        return payload

    def test_minimal_required_fields_round_trip(self):
        model = DremioDatasetResponse.model_validate(self._minimum_payload())
        assert model.resource_id == "rid-1"
        assert model.location_id == "loc-1"
        assert model.columns == []

    def test_missing_required_field_raises(self):
        payload = self._minimum_payload()
        del payload["RESOURCE_ID"]
        with pytest.raises(ValidationError):
            DremioDatasetResponse.model_validate(payload)

    def test_community_edition_null_location_id_accepted(self):
        # Mirrors legacy `.get("LOCATION_ID", "")` behavior — null
        # LOCATION_ID on Community-edition views must not 500.
        model = DremioDatasetResponse.model_validate(
            self._minimum_payload(LOCATION_ID=None)
        )
        assert model.location_id is None


class TestDremioContainerResponseValidation:
    def test_name_backfilled_from_path_list(self):
        model = DremioContainerResponse.model_validate(
            {"containerType": "FOLDER", "path": ["root", "child"]}
        )
        assert model.name == "child"

    def test_name_backfilled_from_path_string(self):
        model = DremioContainerResponse.model_validate(
            {"containerType": "FOLDER", "path": "only"}
        )
        assert model.name == "only"

    def test_missing_name_and_path_raises(self):
        # Refuses to fabricate an "unknown" entity; downstream URN
        # building cannot recover from a corrupted name silently.
        with pytest.raises(ValidationError):
            DremioContainerResponse.model_validate({"containerType": "FOLDER"})

    def test_empty_path_with_no_name_raises(self):
        with pytest.raises(ValidationError):
            DremioContainerResponse.model_validate(
                {"containerType": "FOLDER", "path": []}
            )
