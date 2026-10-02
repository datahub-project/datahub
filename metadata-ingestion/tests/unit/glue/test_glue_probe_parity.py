import re
from typing import TYPE_CHECKING, Dict, Iterator, List

import boto3
import pytest
from moto import mock_aws

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
)

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


def _ingest(config: Dict[str, object]) -> EmittedIndex:
    source = GlueSource(
        config=GlueSourceConfig.model_validate(config),
        ctx=PipelineContext(run_id="glue-probe-parity"),
    )
    index = EmittedIndex.from_workunits(source.get_workunits())
    assert not source.report.failures
    return index


# Notes glue_probe.py gives on a normal run of a recipe that sets the rule;
# none is a degraded fetch. `_note_database_rules`, from `tables --database`:
_RESOURCE_LINK_NOTE = re.compile(
    r"database '[^']+' is a Lake Formation resource link and "
    r"ignore_resource_links is true, so ingestion never lists it or any table in it"
)
# `jobs`, when extract_transforms is false and when catalog_id is set:
_NO_TRANSFORMS_NOTE = (
    "extract_transforms is off, so ingestion emits none of these jobs; they "
    "are listed for inspection only"
)
_JOBS_CATALOG_NOTE = (
    "catalog_id does not apply to jobs: Glue's job API is not cross-account, "
    "so these are the calling account's jobs, and ingestion emits them under "
    "this recipe"
)


def _listings(
    recipe: Dict[str, object], tables_expected: bool = True
) -> List[ParityListing]:
    # Each note is accepted only where the recipe field that triggers it is
    # set: an accepted entry that matched nothing fails as stale.
    no_jobs = recipe.get("extract_transforms") is False
    jobs_accept = ((_NO_TRANSFORMS_NOTE,) if no_jobs else ()) + (
        (_JOBS_CATALOG_NOTE,) if recipe.get("catalog_id") else ()
    )
    return [
        ParityListing("databases", "databases", lambda i: i.container_names()),
        ParityListing(
            "tables",
            "tables",
            lambda i: {
                DatasetUrn.from_string(u).name
                for u in i.urns("dataset", with_aspect=SubTypesClass)
            },
            # Under every listed database, kept or not, so a table excluded
            # only through its database is part of the comparison.
            fan_out=FanOut("databases", "database"),
            identity=lambda r: f"{r.parent_path[-1]}.{r.name}",
            expect_empty=not tables_expected,
            accept_warnings=(
                (_RESOURCE_LINK_NOTE,) if recipe.get("ignore_resource_links") else ()
            ),
        ),
        ParityListing(
            "jobs",
            "jobs",
            lambda i: {DataFlowUrn.from_string(u).flow_id for u in i.urns("dataFlow")},
            expect_empty=no_jobs,
            accept_warnings=jobs_accept,
        ),
    ]


# patterns and ignored resource links: test_each_rule_is_exercised_and_named.
@pytest.mark.parametrize(
    "recipe",
    [
        pytest.param({}, id="defaults"),
        pytest.param(
            {
                "table_pattern": {"allow": [r"^sales\.orders$", r"^ops\..*"]},
                "extract_transforms": False,
            },
            id="qualified-allow-and-no-jobs",
        ),
        pytest.param({"catalog_id": "123456789012"}, id="pinned-own-catalog"),
    ],
)
def test_probe_filter_agrees_with_ingestion(
    catalog: None, recipe: Dict[str, object]
) -> None:
    assert_probe_parity("glue", {**_BASE, **recipe}, _ingest, _listings(recipe))


def test_each_rule_is_exercised_and_named(catalog: None) -> None:
    recipe: Dict[str, object] = {
        "database_pattern": {"deny": ["^scratch$"]},
        "table_pattern": {"deny": [r".*_tmp$"]},
        "ignore_resource_links": True,
    }
    report = assert_probe_parity(
        "glue", {**_BASE, **recipe}, _ingest, _listings(recipe)
    )
    assert report.kinds["tables"].accepted_warnings == (
        "database 'shared_link' is a Lake Formation resource link and "
        "ignore_resource_links is true, so ingestion never lists it or any table in it",
    )
    assert report.excluded_by("databases") == {
        "scratch": "database_pattern",
        "shared_link": "ignore_resource_links",
    }
    assert report.excluded_by("tables") == {
        "sales.orders_tmp": "table_pattern",
        "sales.shared_orders": "ignore_resource_links",
        "scratch.t1": "database_pattern",
        "shared_link.linked_t": "ignore_resource_links",
    }


def test_a_bare_table_name_allow_drops_every_table(catalog: None) -> None:
    # table_pattern is matched on "database.table", so "^orders$" matches
    # nothing and ingestion emits no table at all.
    recipe: Dict[str, object] = {"table_pattern": {"allow": ["^orders$"]}}
    report = assert_probe_parity(
        "glue", {**_BASE, **recipe}, _ingest, _listings(recipe, tables_expected=False)
    )
    assert set(report.excluded_by("tables").values()) == {"table_pattern"}
