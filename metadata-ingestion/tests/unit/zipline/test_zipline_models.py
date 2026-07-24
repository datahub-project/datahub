import pytest

from datahub.ingestion.source.zipline.config import ZiplineConfig, ZiplineOwnerMapping
from datahub.ingestion.source.zipline.constants import (
    operation_to_feature_data_type,
)
from datahub.ingestion.source.zipline.models import (
    Aggregation,
    Derivation,
    EventSource,
    GroupBy,
    MetaData,
    Query,
    Source,
    Window,
)
from datahub.metadata.schema_classes import MLFeatureDataTypeClass


def _agg(input_column, operation, windows=None, buckets=None, arg_map=None):
    return Aggregation(
        input_column=input_column,
        operation=operation,
        windows=windows or [],
        buckets=buckets or [],
        arg_map=arg_map or {},
    )


def test_window_suffix():
    assert Window(length=3, time_unit=1).suffix == "3d"
    assert Window(length=7, time_unit=0).suffix == "7h"


def test_output_column_names_windowed():
    # sum over two day-windows -> one column per window
    agg = _agg(
        "purchase_amount",
        7,
        windows=[Window(length=3, time_unit=1), Window(length=7, time_unit=1)],
    )
    assert agg.output_column_names() == [
        "purchase_amount_sum_3d",
        "purchase_amount_sum_7d",
    ]


def test_output_column_names_no_window():
    agg = _agg("item_id", 5)
    assert agg.output_column_names() == ["item_id_approx_unique_count"]


def test_output_column_names_without_input_column_is_empty():
    # An aggregation with no inputColumn cannot yield a real feature name; the
    # old code produced a bogus "None_<op>" column.
    assert _agg(None, 7).output_column_names() == []


def test_output_column_names_bucketed():
    agg = _agg(
        "amount", 7, windows=[Window(length=1, time_unit=1)], buckets=["country"]
    )
    assert agg.output_column_names() == ["amount_sum_1d_by_country"]


def test_output_column_names_k_operation():
    # last_k (13) uses the k arg as the suffix, mirroring Chronon's _get_op_suffix
    agg = _agg(
        "clicks", 13, windows=[Window(length=1, time_unit=1)], arg_map={"k": "5"}
    )
    assert agg.output_column_names() == ["clicks_last5_1d"]


def test_group_by_feature_names_and_data_types():
    group_by = GroupBy(
        metaData=MetaData(name="my_team.user_features.v1"),
        keyColumns=["user_id"],
        aggregations=[
            _agg("purchase_amount", 7, windows=[Window(length=3, time_unit=1)]),
            _agg("item_id", 5, windows=[Window(length=30, time_unit=1)]),
        ],
    )
    assert group_by.feature_names() == [
        "purchase_amount_sum_3d",
        "item_id_approx_unique_count_30d",
    ]


def test_feature_names_no_aggregations_uses_selects():
    # A keyed GroupBy without aggregations exposes its selected non-key columns.
    group_by = GroupBy(
        metaData=MetaData(name="t.gb.v1"),
        keyColumns=["user_id"],
        sources=[
            Source(
                events=EventSource(
                    table="data.events",
                    query=Query(selects={"event_type": "event_type", "cnt": "count"}),
                )
            )
        ],
    )
    assert group_by.feature_names() == ["event_type", "cnt"]


def test_feature_names_with_wildcard_and_renamed_derivations():
    group_by = GroupBy(
        metaData=MetaData(name="t.gb.v1"),
        keyColumns=["user_id"],
        sources=[
            Source(
                events=EventSource(
                    table="data.events",
                    query=Query(selects={"a": "a", "b": "b"}),
                )
            )
        ],
        derivations=[
            Derivation(name="*", expression="*"),
            Derivation(name="ratio", expression="a / b"),
        ],
    )
    # Wildcard passes through pre-derived columns; the explicit derivation adds
    # its own output name.
    assert group_by.feature_names() == ["a", "b", "ratio"]


def test_metadata_column_tags():
    meta = MetaData(
        name="gb",
        customJson='{"column_tags": {"user_id": {"pii": "true"}, "amt": {"x": "y"}}}',
    )
    assert meta.column_tags() == {
        "user_id": {"pii": "true"},
        "amt": {"x": "y"},
    }


def test_metadata_has_malformed_custom_json():
    # Absent or valid customJson is not malformed; a broken blob is (so the
    # mapper can warn instead of silently dropping tags).
    assert MetaData(name="gb").has_malformed_custom_json() is False
    assert (
        MetaData(
            name="gb", customJson='{"groupby_tags": {}}'
        ).has_malformed_custom_json()
        is False
    )
    assert (
        MetaData(name="gb", customJson="{not valid json").has_malformed_custom_json()
        is True
    )


def test_metadata_output_table_name_sanitized():
    meta = MetaData(name="my_team.user_features.v1", outputNamespace="my_team_features")
    assert meta.output_table_name() == "my_team_features.my_team_user_features_v1"


def test_metadata_tags_from_custom_json():
    meta = MetaData(
        name="gb",
        customJson='{"groupby_tags": {"tier": "gold", "pii": "false"}}',
    )
    assert meta.tags("groupby_tags") == {"tier": "gold", "pii": "false"}


def test_operation_to_feature_data_type():
    assert operation_to_feature_data_type(6) == MLFeatureDataTypeClass.COUNT
    assert operation_to_feature_data_type(7) == MLFeatureDataTypeClass.CONTINUOUS
    assert operation_to_feature_data_type(13) == MLFeatureDataTypeClass.SEQUENCE
    assert operation_to_feature_data_type(999) == MLFeatureDataTypeClass.UNKNOWN


def test_config_owner_extraction_requires_mappings():
    with pytest.raises(ValueError):
        ZiplineConfig(path="/tmp/x", enable_owner_extraction=True)


def test_owner_mapping_rejects_unknown_ownership_type():
    with pytest.raises(ValueError):
        ZiplineOwnerMapping(
            team_name="t",
            datahub_owner_urn="urn:li:corpGroup:t",
            datahub_ownership_type="OWNER",
        )


def test_owner_mapping_accepts_ownership_type_urn():
    mapping = ZiplineOwnerMapping(
        team_name="t",
        datahub_owner_urn="urn:li:corpGroup:t",
        datahub_ownership_type="urn:li:ownershipType:custom",
    )
    assert mapping.datahub_ownership_type == "urn:li:ownershipType:custom"


def test_owner_mapping_rejects_typoed_key():
    # extra="forbid" turns a mistyped key into a config error instead of silently
    # yielding zero owners.
    with pytest.raises(ValueError):
        ZiplineOwnerMapping.model_validate(
            {"team_name": "t", "datahub_owner": "urn:li:corpGroup:t"}
        )
