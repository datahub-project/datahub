from datahub.ingestion.source.workday.config import (
    DataSourcePlatformConfig,
    WorkdayConfig,
)
from datahub.ingestion.source.workday.lineage import resolve_external_upstream_urn
from datahub.ingestion.source.workday.mapper import WorkdayMapper, _parse_iso_millis
from datahub.ingestion.source.workday.models import (
    CustomReport,
    PrismBucket,
    PrismDataset,
    PrismDataSource,
    PrismField,
    PrismTable,
    WqlDataSource,
)
from datahub.ingestion.source.workday.report import WorkdayReport
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OperationClass,
    OwnershipClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)


def _config(**overrides: object) -> WorkdayConfig:
    base: dict = {
        "base_url": "https://wd2-impl-services1.workday.com/",
        "tenant": "acme",
        "client_id": "client",
        "client_secret": "secret",
    }
    base.update(overrides)
    return WorkdayConfig.model_validate(base)


def test_base_url_trailing_slash_is_normalized() -> None:
    assert _config().base_url == "https://wd2-impl-services1.workday.com"


def test_field_normalizer_resolves_aliases_and_type() -> None:
    field = PrismField.model_validate(
        {
            "displayName": "as_of_date",
            "type": {"descriptor": "Date"},
            "isPrimaryKey": True,
        }
    )
    assert field.name == "as_of_date"
    assert field.type_descriptor == "Date"
    assert field.is_primary_key is True


def test_table_normalizer_pulls_source_type_and_ids() -> None:
    table = PrismTable.model_validate(
        {
            "id": "tbl-1",
            "name": "Worker_Headcount",
            "sourceType": "Workday",
            "schema": {"fields": [{"name": "count", "type": "numeric"}]},
            "dataSources": [{"id": "ds-1"}, "ds-2"],
            "datasetId": "dset-1",
        }
    )
    assert table.source_type == "Workday"
    assert table.dataset_id == "dset-1"
    assert table.data_source_ids == ["ds-1", "ds-2"]
    assert [f.name for f in table.fields] == ["count"]


def test_external_upstream_resolves_only_when_mapped() -> None:
    config = _config(
        data_source_platform_mapping={
            "External Snowflake": DataSourcePlatformConfig(
                platform="snowflake",
                dataset_name="DB.Schema.Headcount",
            )
        },
        env="PROD",
    )
    # Mapped: resolves to a lowercased Snowflake dataset URN.
    urn = resolve_external_upstream_urn("External Snowflake", config)
    assert urn is not None
    assert "snowflake" in urn
    assert "db.schema.headcount" in urn
    # Unmapped: stays a native Workday object (no external URN).
    assert resolve_external_upstream_urn("Some Workday Source", config) is None


def test_external_upstream_preserves_case_when_configured() -> None:
    config = _config(
        data_source_platform_mapping={
            "S": DataSourcePlatformConfig(
                platform="snowflake",
                dataset_name="DB.Schema.T",
                convert_urns_to_lowercase=False,
            )
        }
    )
    urn = resolve_external_upstream_urn("S", config)
    assert urn is not None
    assert "DB.Schema.T" in urn


def test_unknown_field_type_falls_back_to_string_and_is_reported() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    schema = mapper._schema_aspect(
        "urn:li:dataset:(x)",
        [PrismField.model_validate({"name": "weird", "type": "quaternion"})],
    )
    assert schema is not None
    assert schema.fields[0].fieldPath == "weird"
    assert report.unknown_field_types == 1


def test_data_source_report_vs_external_subtype() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    workday_source = PrismDataSource.model_validate(
        {"id": "ds-1", "name": "Headcount Report", "sourceType": "Workday"}
    )
    id_to_urn = {"ds-1": mapper.dataset_urn("source", "ds-1")}
    list(mapper.map_data_source(workday_source, id_to_urn))
    assert report.reports_scanned == 1
    assert report.data_sources_scanned == 0


def test_parse_iso_millis_handles_z_and_bad_values() -> None:
    assert _parse_iso_millis("2024-01-01T00:00:00Z") == 1704067200000
    # Naive timestamps are treated as UTC; unparseable values are dropped.
    assert _parse_iso_millis("2024-01-01T00:00:00") == 1704067200000
    assert _parse_iso_millis("not-a-date") is None
    assert _parse_iso_millis(None) is None


def test_schema_field_carries_precision_scale_and_nullability() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    numeric = mapper._schema_field(
        PrismField.model_validate(
            {"name": "amount", "type": "numeric", "precision": 10, "scale": 2}
        )
    )
    assert numeric.nativeDataType == "numeric(10,2)"
    assert numeric.nullable is True
    # A primary key is non-nullable regardless of the reported flag.
    pk = mapper._schema_field(
        PrismField.model_validate(
            {"name": "id", "type": "text", "isPrimaryKey": True, "nullable": True}
        )
    )
    assert pk.nullable is False
    assert pk.isPartOfKey is True


def test_column_level_lineage_from_dataset_field_mappings() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    table = PrismTable.model_validate(
        {"id": "tbl-1", "name": "T", "fields": [{"name": "out_col"}]}
    )
    dataset = PrismDataset.model_validate(
        {
            "id": "dset-1",
            "name": "D",
            "outputTableId": "tbl-1",
            "fieldMappings": [
                {
                    "outputField": "out_col",
                    "sourceField": "src",
                    "sourceObjectId": "ds-1",
                },
                # Missing source object: not resolvable, so no edge is emitted.
                {"outputField": "orphan", "sourceField": "x"},
            ],
        }
    )
    id_to_urn = {
        "tbl-1": mapper.dataset_urn("table", "tbl-1"),
        "ds-1": mapper.dataset_urn("source", "ds-1"),
    }
    edges = mapper._fine_grained_lineage(table, dataset, id_to_urn)
    assert len(edges) == 1
    downstreams = edges[0].downstreams or []
    upstreams = edges[0].upstreams or []
    assert "out_col" in downstreams[0]
    assert "src" in upstreams[0]


def test_profile_and_operation_emitted_from_table_detail() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    table = PrismTable.model_validate(
        {
            "id": "tbl-1",
            "name": "T",
            "rows": 42,
            "lastRefreshed": "2024-06-01T00:00:00Z",
            "fields": [{"name": "a"}, {"name": "b"}],
        }
    )
    profile = mapper._profile_aspect(table)
    assert isinstance(profile, DatasetProfileClass)
    assert profile.rowCount == 42
    assert profile.columnCount == 2
    operation = mapper._operation_aspect(table)
    assert isinstance(operation, OperationClass)
    assert operation.lastUpdatedTimestamp == 1717200000000


def test_transformation_logic_becomes_view_properties() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    dataset = PrismDataset.model_validate(
        {"id": "dset-1", "name": "D", "dpl": "FORMAT x"}
    )
    id_to_urn = {"dset-1": mapper.dataset_urn("dataset", "dset-1")}
    aspects = [
        wu.metadata.aspect
        for wu in mapper.map_dataset(dataset, id_to_urn, {})
        if hasattr(wu.metadata, "aspect")
    ]
    view_props = [a for a in aspects if isinstance(a, ViewPropertiesClass)]
    assert len(view_props) == 1
    assert view_props[0].viewLogic == "FORMAT x"


def test_business_object_emits_schema() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    business_object = WqlDataSource.model_validate(
        {"id": "bo-1", "descriptor": "Worker", "fields": [{"name": "f"}]}
    )
    id_to_urn = {"bo-1": mapper.dataset_urn("businessObject", "bo-1")}
    list(mapper.map_business_object(business_object, id_to_urn))
    assert report.business_objects_scanned == 1
    assert report.fields_scanned == 1


def test_custom_report_links_to_business_object_with_field_cll() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    custom_report = CustomReport.model_validate(
        {
            "id": "rpt-1",
            "name": "R",
            "dataSource": "Worker",
            "outputFields": [{"name": "org"}, {"name": "computed_total"}],
        }
    )
    business_object = WqlDataSource.model_validate(
        {"id": "bo-1", "descriptor": "Worker", "fields": [{"name": "org"}]}
    )
    bo_urn = mapper.dataset_urn("businessObject", "bo-1")
    id_to_urn = {
        "rpt-1": mapper.dataset_urn("report", "rpt-1"),
        "bo-1": bo_urn,
    }
    aspects = [
        wu.metadata.aspect
        for wu in mapper.map_custom_report(
            custom_report, id_to_urn, {"Worker": business_object}
        )
        if hasattr(wu.metadata, "aspect")
    ]
    lineage = [a for a in aspects if isinstance(a, UpstreamLineageClass)]
    assert len(lineage) == 1
    assert lineage[0].upstreams[0].dataset == bo_urn
    # Only the field present on both sides ("org") produces a column-level edge.
    fgl = lineage[0].fineGrainedLineages or []
    assert len(fgl) == 1
    downstreams = fgl[0].downstreams or []
    assert "org" in downstreams[0]
    # The report also carries its own schema from its output fields.
    schema = [a for a in aspects if isinstance(a, SchemaMetadataClass)]
    assert len(schema) == 1
    assert {f.fieldPath for f in schema[0].fields} == {"org", "computed_total"}


def test_owner_group_emitted_alongside_user_owner() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    obj = PrismTable.model_validate(
        {"id": "t", "name": "T", "createdBy": "analyst", "ownerGroup": "HR Admins"}
    )
    ownership = mapper._ownership_aspect(obj)
    assert isinstance(ownership, OwnershipClass)
    owners = {o.owner for o in ownership.owners}
    assert "urn:li:corpuser:analyst" in owners
    assert "urn:li:corpGroup:HR Admins" in owners


def test_dataset_tags_and_glossary_terms_are_emitted() -> None:
    mapper = WorkdayMapper(_config(), WorkdayReport())
    table = PrismTable.model_validate(
        {
            "id": "t",
            "name": "T",
            "tags": ["PII", "Certified"],
            "glossaryTerms": [{"descriptor": "Headcount"}],
        }
    )
    aspects = [
        wu.metadata.aspect
        for wu in mapper.map_table(table, {"t": mapper.dataset_urn("table", "t")}, {})
        if hasattr(wu.metadata, "aspect")
    ]
    tags = [a for a in aspects if isinstance(a, GlobalTagsClass)]
    assert tags and {t.tag for t in tags[0].tags} == {
        "urn:li:tag:PII",
        "urn:li:tag:Certified",
    }
    terms = [a for a in aspects if isinstance(a, GlossaryTermsClass)]
    assert terms and terms[0].terms[0].urn == "urn:li:glossaryTerm:Headcount"


def test_domain_is_applied_to_matching_dataset() -> None:
    from datahub.configuration.common import AllowDenyPattern

    mapper = WorkdayMapper(
        _config(domain={"Finance": AllowDenyPattern(allow=[".*Ledger.*"])}),
        WorkdayReport(),
    )
    matched = [
        wu.metadata.aspect
        for wu in mapper.map_table(
            PrismTable.model_validate({"id": "t", "name": "General Ledger"}),
            {"t": mapper.dataset_urn("table", "t")},
            {},
        )
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DomainsClass)
    ]
    assert matched and matched[0].domains == ["urn:li:domain:Finance"]


def test_business_object_relationship_lineage() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    business_object = WqlDataSource.model_validate(
        {
            "id": "bo-1",
            "descriptor": "Worker",
            "relatedBusinessObjects": [{"id": "bo-2"}, "bo-missing"],
        }
    )
    id_to_urn = {
        "bo-1": mapper.dataset_urn("businessObject", "bo-1"),
        "bo-2": mapper.dataset_urn("businessObject", "bo-2"),
    }
    aspects = [
        wu.metadata.aspect
        for wu in mapper.map_business_object(business_object, id_to_urn)
        if hasattr(wu.metadata, "aspect")
    ]
    lineage = [a for a in aspects if isinstance(a, UpstreamLineageClass)]
    # Only the resolvable related object ("bo-2") yields an edge.
    assert len(lineage) == 1
    assert lineage[0].upstreams[0].dataset == id_to_urn["bo-2"]
    assert report.related_object_edges_emitted == 1


def test_bucket_is_upstream_of_its_target_table() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    bucket = PrismBucket.model_validate(
        {"id": "bkt-1", "name": "upload", "targetTableId": "tbl-1", "state": "Success"}
    )
    bucket_urn = mapper.dataset_urn("bucket", "bkt-1")
    id_to_urn = {"tbl-1": mapper.dataset_urn("table", "tbl-1"), "bkt-1": bucket_urn}
    assert bucket.target_table_id == "tbl-1"
    list(mapper.map_bucket(bucket, id_to_urn))
    assert report.buckets_scanned == 1

    table = PrismTable.model_validate(
        {"id": "tbl-1", "name": "T", "fields": [{"name": "a"}]}
    )
    aspects = [
        wu.metadata.aspect
        for wu in mapper.map_table(
            table, id_to_urn, {}, extra_upstream_urns=[bucket_urn]
        )
        if hasattr(wu.metadata, "aspect")
    ]
    lineage = [a for a in aspects if isinstance(a, UpstreamLineageClass)]
    assert lineage and lineage[0].upstreams[0].dataset == bucket_urn
