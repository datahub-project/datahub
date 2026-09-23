from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_source,
    make_assertion_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParametersClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
)

# Shared by the run-event builder, status/operator helpers, and governance-table
# extractor added in later tasks of this seam.
DATABRICKS_PLATFORM = "databricks"


@dataclass
class StdAssertion:
    # Same shape as dbt AssertionParams / GX DataHubStdAssertion (reused, not new vocabulary).
    scope: str
    operator: str = AssertionStdOperatorClass._NATIVE_
    aggregation: str = AssertionStdAggregationClass._NATIVE_
    parameters: Optional[AssertionStdParametersClass] = None


def make_urn(key: Dict[str, str]) -> str:
    # Key includes a "surface" tag so GUIDs cannot collide across surfaces, plus the
    # dataset URN (which already encodes platform_instance/metastore/env).
    return make_assertion_urn(datahub_guid(key))


def build_custom_assertion_info(
    *,
    assertion_urn: str,
    entity_urn: str,
    category: str,
    display_name: str,
    native_type: str,
    std: StdAssertion,
    field_urns: Optional[List[str]] = None,
    logic: Optional[str] = None,
    native_parameters: Optional[Dict[str, str]] = None,
    external_url: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
) -> MetadataChangeProposalWrapper:
    fields = field_urns or []
    info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        description=display_name,  # the assertions list renders the name from description
        externalUrl=external_url,
        customProperties=custom_properties or None,
        source=make_assertion_source(),  # EXTERNAL source (same call the existing PRs use)
        customAssertion=CustomAssertionInfoClass(
            type=category,
            entity=entity_urn,
            field=fields[0] if fields else None,
            fields=fields or None,
            scope=std.scope,
            aggregation=std.aggregation,
            operator=std.operator,
            parameters=std.parameters,
            nativeType=native_type,
            nativeParameters=native_parameters,
            logic=logic,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info)
