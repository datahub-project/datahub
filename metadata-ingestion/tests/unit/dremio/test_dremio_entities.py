import pytest

from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioContainer,
    DremioFolder,
    DremioQuery,
    DremioSourceContainer,
    DremioSpace,
)

VALID_TS = "2024-01-15 10:30:00.000"


class TestDremioQueryParsing:
    def test_queried_datasets_parsed_from_brackets(self):
        q = DremioQuery(
            job_id="j1",
            username="u",
            submitted_ts=VALID_TS,
            query="SELECT 1",
            queried_datasets="[space.schema.table1,space.schema.table2]",
        )
        assert "space.schema.table1" in q.queried_datasets
        assert "space.schema.table2" in q.queried_datasets

    def test_queried_datasets_single_entry(self):
        q = DremioQuery(
            job_id="j1",
            username="u",
            submitted_ts=VALID_TS,
            query="SELECT 1",
            queried_datasets="[myspace.table]",
        )
        assert "myspace.table" in q.queried_datasets


class TestDremioCatalogIsValidQuery:
    """Tests for DremioCatalog.is_valid_query() required-fields check."""

    VALID_QUERY = {
        "job_id": "abc123",
        "user_name": "jdoe",
        "submitted_ts": "2024-01-15 10:00:00.000",
        "query": "SELECT 1",
        "queried_datasets": "[schema.table]",
    }

    def test_valid_query_passes(self):
        catalog = DremioCatalog.__new__(DremioCatalog)
        assert catalog.is_valid_query(self.VALID_QUERY) is True

    @pytest.mark.parametrize(
        "missing_field",
        ["job_id", "user_name", "submitted_ts", "query", "queried_datasets"],
    )
    def test_missing_required_field_fails(self, missing_field):
        catalog = DremioCatalog.__new__(DremioCatalog)
        query = {k: v for k, v in self.VALID_QUERY.items() if k != missing_field}
        assert catalog.is_valid_query(query) is False

    def test_empty_string_field_fails(self):
        catalog = DremioCatalog.__new__(DremioCatalog)
        query = {**self.VALID_QUERY, "query": ""}
        assert catalog.is_valid_query(query) is False


class TestDremioContainerSubclassDeclaration:
    """DremioContainer.subclass is declared without a default so a new
    subclass that forgets to set it can't silently inherit DREMIO_FOLDER."""

    def test_known_subclasses_declare_subtype(self):
        assert DremioSourceContainer.subclass == DatasetContainerSubTypes.DREMIO_SOURCE
        assert DremioSpace.subclass == DatasetContainerSubTypes.DREMIO_SPACE
        assert DremioFolder.subclass == DatasetContainerSubTypes.DREMIO_FOLDER

    def test_base_class_has_no_subclass_default(self):
        # If a subclass forgets to set subclass, instance.subclass raises
        # AttributeError rather than silently returning DREMIO_FOLDER.
        with pytest.raises(AttributeError):
            DremioContainer.subclass  # noqa: B018

    def test_undeclared_subclass_raises_on_attribute_access(self):
        class UndeclaredContainer(DremioContainer):
            pass

        with pytest.raises(AttributeError):
            UndeclaredContainer.subclass  # noqa: B018
