from typing import TYPE_CHECKING, Dict, Iterator, Mapping, Set, Tuple

import boto3
import pytest
from moto import mock_aws

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.metadata.schema_classes import ContainerPropertiesClass, SubTypesClass
from datahub.metadata.urns import DataFlowUrn, DatasetUrn

if TYPE_CHECKING:
    from mypy_boto3_glue.type_defs import StorageDescriptorTypeDef

_REGION = "us-east-1"
_OWNER = "222222222222"
_BASE: Dict[str, object] = {
    "aws_region": _REGION,
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
    "use_s3_bucket_tags": False,
    "use_s3_object_tags": False,
    "include_view_lineage": False,
    "resolve_resource_link_schema": False,
}

Emitted = Tuple[Set[str], Set[str], Set[str]]


@pytest.fixture
def catalog() -> Iterator[None]:
    """sales: a table, a *_tmp table, a view and a table-level resource link;
    ops and scratch: one table each; shared_link: a database-level resource
    link with one table. Two jobs whose scripts are missing, so ingestion
    emits each as a DataFlow with one DataJob (moto has no
    get_dataflow_graph)."""
    with mock_aws():
        glue = boto3.client(
            "glue",
            region_name=_REGION,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        for name in ("sales", "ops", "scratch"):
            glue.create_database(DatabaseInput={"Name": name})
        glue.create_database(
            DatabaseInput={
                "Name": "shared_link",
                "TargetDatabase": {"CatalogId": _OWNER, "DatabaseName": "owner_db"},
            }
        )
        columns: "StorageDescriptorTypeDef" = {
            "Columns": [{"Name": "id", "Type": "int"}]
        }
        for database, table in (
            ("sales", "orders"),
            ("sales", "orders_tmp"),
            ("ops", "runs"),
            ("scratch", "t1"),
            ("shared_link", "linked_t"),
        ):
            glue.create_table(
                DatabaseName=database,
                TableInput={"Name": table, "StorageDescriptor": columns},
            )
        glue.create_table(
            DatabaseName="sales",
            TableInput={
                "Name": "orders_view",
                "TableType": "VIRTUAL_VIEW",
                "ViewOriginalText": "SELECT id FROM sales.orders",
                "StorageDescriptor": columns,
            },
        )
        glue.create_table(
            DatabaseName="sales",
            TableInput={
                "Name": "shared_orders",
                "TargetTable": {
                    "CatalogId": _OWNER,
                    "DatabaseName": "owner_db",
                    "Name": "orders",
                },
            },
        )
        for job in ("nightly_load", "hourly_sync"):
            glue.create_job(
                Name=job,
                Role="arn:aws:iam::123456789012:role/glue-job-role",
                Command={
                    "Name": "glueetl",
                    "ScriptLocation": f"s3://missing-bucket/{job}.py",
                },
            )
        yield


def _ingested(recipe: Mapping[str, object]) -> Emitted:
    source = GlueSource(
        config=GlueSourceConfig.model_validate({**_BASE, **recipe}),
        ctx=PipelineContext(run_id="glue-probe-parity"),
    )
    databases: Set[str] = set()
    datasets: Set[str] = set()
    flows: Set[str] = set()
    for wu in source.get_workunits():
        urn = wu.get_urn()
        properties = wu.get_aspect_of_type(ContainerPropertiesClass)
        if properties is not None:
            databases.add(properties.name)
        elif urn.startswith("urn:li:dataset:") and wu.get_aspect_of_type(SubTypesClass):
            datasets.add(DatasetUrn.from_string(urn).name)
        elif urn.startswith("urn:li:dataFlow:"):
            flows.add(DataFlowUrn.from_string(urn).flow_id)
    assert not source.report.failures
    return databases, datasets, flows


def _included(
    recipe: Dict[str, object], command: str, kwargs: Dict[str, object]
) -> Set[str]:
    run = run_probe_method("glue", recipe, command, kwargs)
    listing = listing_from_run(run.to_dict())
    assert listing.kind is not None and not listing.truncated
    verdicts = check_filters(
        source_type="glue",
        config_dict=recipe,
        kind=listing.kind,
        parent_path=listing.parent_path,
        names=listing.names,
        attributes=listing.attributes,
    )
    return {r.name for r in verdicts.results if r.included}


def _probed(recipe: Mapping[str, object]) -> Emitted:
    config = {**_BASE, **recipe}
    listed = run_probe_method("glue", config, "databases", {}).result
    assert isinstance(listed, list)
    every_database = [r["name"] for r in listed]
    databases = _included(config, "databases", {})
    # Every listed database, kept or not, so a table excluded only through its
    # database is part of the comparison.
    datasets = {
        f"{database}.{table}"
        for database in every_database
        for table in _included(config, "tables", {"database": database})
    }
    flows = _included(config, "jobs", {})
    return databases, datasets, flows


@pytest.mark.parametrize(
    "recipe",
    [
        pytest.param({}, id="defaults"),
        pytest.param(
            {
                "database_pattern": {"deny": ["^scratch$"]},
                "table_pattern": {"deny": [r".*_tmp$"]},
                "ignore_resource_links": True,
            },
            id="patterns-and-ignored-links",
        ),
        pytest.param(
            {
                "table_pattern": {"allow": [r"^sales\.orders$", r"^ops\..*"]},
                "extract_transforms": False,
            },
            id="qualified-allow-and-no-jobs",
        ),
        pytest.param({"table_pattern": {"allow": ["^orders$"]}}, id="bare-name-allow"),
        pytest.param({"catalog_id": "123456789012"}, id="pinned-own-catalog"),
    ],
)
def test_probe_filter_agrees_with_ingestion(
    catalog: None, recipe: Dict[str, object]
) -> None:
    assert _probed(recipe) == _ingested(recipe)


def test_the_fixture_exercises_every_rule(catalog: None) -> None:
    """Guard against a parity test that agrees about nothing."""
    databases, datasets, flows = _ingested(
        {
            "database_pattern": {"deny": ["^scratch$"]},
            "table_pattern": {"deny": [r".*_tmp$"]},
            "ignore_resource_links": True,
        }
    )

    assert databases == {"sales", "ops"}
    assert datasets == {"sales.orders", "sales.orders_view", "ops.runs"}
    assert flows == {"nightly_load", "hourly_sync"}
