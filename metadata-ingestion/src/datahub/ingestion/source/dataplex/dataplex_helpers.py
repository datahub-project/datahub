"""Helper functions and utilities for Dataplex source."""

from typing import Dict, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

# Type mapping from Dataplex to DataHub schema types
DATAPLEX_TYPE_MAPPING = {
    "BOOL": BooleanTypeClass(),
    "BYTE": BytesTypeClass(),
    "INT16": NumberTypeClass(),
    "INT32": NumberTypeClass(),
    "INT64": NumberTypeClass(),
    "FLOAT": NumberTypeClass(),
    "DOUBLE": NumberTypeClass(),
    "DECIMAL": NumberTypeClass(),
    "STRING": StringTypeClass(),
    "BINARY": BytesTypeClass(),
    "TIMESTAMP": TimeTypeClass(),
    "DATE": DateTypeClass(),
    "TIME": TimeTypeClass(),
    "RECORD": RecordTypeClass(),
    "NULL": NullTypeClass(),
}


def make_project_container_urn(project_id: str) -> str:
    """Create URN for the GCP project container."""
    return builder.make_container_urn(project_id)


def make_lake_domain_urn(project_id: str, lake_id: str) -> str:
    """Create URN for a lake as a domain."""
    return builder.make_domain_urn(f"{project_id}.{lake_id}")


def make_zone_domain_urn(project_id: str, lake_id: str, zone_id: str) -> str:
    """Create URN for a zone as a sub-domain."""
    return builder.make_domain_urn(f"{project_id}.{lake_id}.{zone_id}")


def make_entity_dataset_urn(
    entity_id: str, platform: str, project_id: str, env: str
) -> str:
    """Create dataset URN for a Dataplex entity."""
    dataset_name = f"{project_id}.{entity_id}"
    return builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=dataset_name,
        platform_instance=None,
        env=env,
    )


def make_audit_stamp(timestamp) -> Optional[Dict]:
    """Create audit stamp from GCP timestamp."""
    if timestamp:
        return {
            "time": int(timestamp.timestamp() * 1000),
            "actor": "urn:li:corpuser:dataplex",
        }
    return None


def make_dataplex_external_url(
    resource_type: str, resource_id: str, project_id: str, location: str, base_url: str
) -> str:
    """Generate external URL for Dataplex console."""
    return (
        f"{base_url}/{resource_type}/locations/{location}/"
        f"{resource_type}/{resource_id}?project={project_id}"
    )


def determine_entity_platform(
    entity: dataplex_v1.Entity,
    project_id: str,
    lake_id: str,
    zone_id: str,
    location: str,
    dataplex_client: dataplex_v1.DataplexServiceClient,
) -> str:
    """Determine the platform (bigquery, gcs, dataplex) for an entity based on its asset."""
    platform = "dataplex"

    if not entity.asset:
        return platform

    try:
        asset_id = entity.asset
        asset_name = f"projects/{project_id}/locations/{location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
        asset_request = dataplex_v1.GetAssetRequest(name=asset_name)
        asset = dataplex_client.get_asset(request=asset_request)

        if asset.resource_spec:
            resource_type = asset.resource_spec.type_.name
            if resource_type == "BIGQUERY_DATASET":
                platform = "bigquery"
            elif resource_type == "STORAGE_BUCKET":
                platform = "gcs"
    except exceptions.GoogleAPICallError:
        # Return default platform if we can't determine from asset
        pass
    except AttributeError:
        # Return default platform if asset structure is unexpected
        pass

    return platform


def map_dataplex_type_to_datahub(type_name: str) -> SchemaFieldDataTypeClass:
    """Map Dataplex type name to DataHub schema field data type."""
    datahub_type = DATAPLEX_TYPE_MAPPING.get(type_name, StringTypeClass())
    return SchemaFieldDataTypeClass(type=datahub_type)


def map_dataplex_field_to_datahub(
    field: dataplex_v1.types.Schema.SchemaField,
) -> SchemaFieldDataTypeClass:
    """Map Dataplex field (with mode) to DataHub field type."""
    type_name = dataplex_v1.types.Schema.Type(field.type_).name
    mode = dataplex_v1.types.Schema.Mode(field.mode).name

    # Handle array types (REPEATED mode)
    if mode == "REPEATED":
        inner_type = map_dataplex_type_to_datahub(type_name)
        return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=[inner_type]))

    return map_dataplex_type_to_datahub(type_name)
