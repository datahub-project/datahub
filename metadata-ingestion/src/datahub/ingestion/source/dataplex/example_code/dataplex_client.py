"""
Dataplex Client Module

Provides DataplexClient class for interacting with Google Cloud Dataplex API.
Handles all API operations for lakes, zones, assets, entities, entry groups,
entries, and data scans (quality & profiling).
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.cloud.datacatalog_lineage_v1 import (
    EntityReference,
    LineageClient,
    SearchLinksRequest,
)

logger = logging.getLogger(__name__)

# Constants
LATEST_JOB_PAGE_SIZE = 1


class DataplexClient:
    """Client for interacting with Google Dataplex API"""

    def __init__(self, project_id: str, location: str = "us-central1"):
        """
        Initialize Dataplex client

        Args:
            project_id: GCP project ID
            location: GCP region (default: us-central1)
        """
        if not project_id or not project_id.strip():
            raise ValueError("project_id cannot be empty")
        if not location or not location.strip():
            raise ValueError("location cannot be empty")

        self.project_id = project_id
        self.location = location
        self.dataplex_client = dataplex_v1.DataplexServiceClient()
        self.metadata_client = dataplex_v1.MetadataServiceClient()
        self.catalog_client = dataplex_v1.CatalogServiceClient()
        self.datascan_client = dataplex_v1.DataScanServiceClient()
        self.lineage_client = LineageClient()

    def extract_lineage_for_entity(
        self, fully_qualified_name: str
    ) -> Dict[str, List[str]]:
        """
        Extract lineage (upstream and downstream links) for a specific entity

        Args:
            fully_qualified_name: The fully qualified name of the entity (e.g., "bigquery:project.dataset.table")

        Returns:
            Dictionary with 'upstream' and 'downstream' lists of fully qualified names
        """
        lineage_data = {"upstream": [], "downstream": []}

        parent = f"projects/{self.project_id}/locations/{self.location}"

        # Get downstream links (where this entity is the source)
        try:
            source = EntityReference()
            source.fully_qualified_name = fully_qualified_name

            request = SearchLinksRequest(parent=parent, source=source)

            for link in self.lineage_client.search_links(request=request):
                if link.target and link.target.fully_qualified_name:
                    lineage_data["downstream"].append(link.target.fully_qualified_name)
        except exceptions.GoogleAPICallError as e:
            logger.debug(f"No downstream lineage found for {fully_qualified_name}: {e}")
        except Exception as e:
            logger.warning(
                f"Error getting downstream lineage for {fully_qualified_name}: {e}"
            )

        # Get upstream links (where this entity is the target)
        try:
            target = EntityReference()
            target.fully_qualified_name = fully_qualified_name

            request = SearchLinksRequest(parent=parent, target=target)

            for link in self.lineage_client.search_links(request=request):
                if link.source and link.source.fully_qualified_name:
                    lineage_data["upstream"].append(link.source.fully_qualified_name)
        except exceptions.GoogleAPICallError as e:
            logger.debug(f"No upstream lineage found for {fully_qualified_name}: {e}")
        except Exception as e:
            logger.warning(
                f"Error getting upstream lineage for {fully_qualified_name}: {e}"
            )

        return lineage_data

    def _extract_entity_full_details(
        self, entity: Any, lake_id: str, zone_id: str
    ) -> Any:
        """
        Fetch full entity details including schema and aspects

        Args:
            entity: Basic entity object
            lake_id: Lake ID
            zone_id: Zone ID

        Returns:
            Full entity object or original if fetch fails
        """
        try:
            get_entity_request = dataplex_v1.GetEntityRequest(
                name=entity.name, view=dataplex_v1.GetEntityRequest.EntityView.FULL
            )
            return self.metadata_client.get_entity(request=get_entity_request)
        except exceptions.GoogleAPICallError as e:
            logger.warning(
                f"        Could not fetch full entity details for {entity.id}: {e}"
            )
            return entity

    def _determine_platform_from_asset(
        self, entity: Any, lake_id: str, zone_id: str
    ) -> str:
        """
        Determine the platform (bigquery, gcs, etc.) from the entity's asset

        Args:
            entity: Entity object
            lake_id: Lake ID
            zone_id: Zone ID

        Returns:
            Platform name (default: "dataplex")
        """
        platform = "dataplex"
        if not entity.asset:
            return platform

        try:
            asset_id_for_entity = entity.asset
            asset_name = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id_for_entity}"
            asset_request = dataplex_v1.GetAssetRequest(name=asset_name)
            asset = self.dataplex_client.get_asset(request=asset_request)

            if asset.resource_spec:
                resource_type = asset.resource_spec.type_.name
                if resource_type == "BIGQUERY_DATASET":
                    platform = "bigquery"
                elif resource_type == "STORAGE_BUCKET":
                    platform = "gcs"
        except exceptions.GoogleAPICallError as e:
            logger.warning(
                f"        Could not determine platform for entity {entity.id}: {e}"
            )
        except AttributeError as e:
            logger.warning(
                f"        Unexpected asset structure for entity {entity.id}: {e}"
            )

        return platform

    def _extract_schema_fields(self, entity: Any) -> List[Dict[str, str]]:
        """
        Extract schema fields from an entity

        Args:
            entity: Entity object with schema

        Returns:
            List of field dictionaries
        """
        schema_fields = []
        if entity.schema and entity.schema.fields:
            for field in entity.schema.fields:
                field_data = {
                    "name": field.name,
                    "type": dataplex_v1.types.Schema.Type(field.type_).name,
                    "mode": dataplex_v1.types.Schema.Mode(field.mode).name,
                    "description": field.description or "",
                }
                schema_fields.append(field_data)
        return schema_fields

    def _extract_aspects(self, obj: Any, obj_type: str = "entity") -> Dict[str, Any]:
        """
        Extract aspects (custom metadata) from an entity or entry

        Args:
            obj: Entity or Entry object
            obj_type: Type of object for logging ("entity" or "entry")

        Returns:
            Dictionary of aspects
        """
        aspects = {}
        if not hasattr(obj, "aspects") or not obj.aspects:
            return aspects

        from google.protobuf import json_format

        for aspect_key, aspect_value in obj.aspects.items():
            try:
                # Try to convert the entire aspect_value to dict first
                try:
                    aspect_dict = json_format.MessageToDict(aspect_value._pb)
                except (AttributeError, TypeError):
                    # If that fails, build it manually
                    aspect_dict = {
                        "aspect_type": aspect_value.aspect_type
                        if hasattr(aspect_value, "aspect_type")
                        else "",
                        "path": aspect_value.path
                        if hasattr(aspect_value, "path")
                        else "",
                        "create_time": str(aspect_value.create_time)
                        if hasattr(aspect_value, "create_time")
                        else "",
                        "update_time": str(aspect_value.update_time)
                        if hasattr(aspect_value, "update_time")
                        else "",
                    }

                    # Try to extract data field
                    if hasattr(aspect_value, "data") and aspect_value.data:
                        try:
                            aspect_dict["data"] = json_format.MessageToDict(
                                aspect_value.data._pb
                            )
                        except (AttributeError, TypeError):
                            aspect_dict["data"] = str(aspect_value.data)

                aspects[aspect_key] = aspect_dict
            except Exception as e:
                logger.warning(
                    f"        Error extracting aspect {aspect_key} from {obj_type}: {e}"
                )
                aspects[aspect_key] = {
                    "error": str(e),
                    "aspect_key": aspect_key,
                    "aspect_type": str(type(aspect_value)),
                }

        return aspects

    def _extract_entities_for_zone(
        self, lake_id: str, zone_id: str
    ) -> List[Dict[str, Any]]:
        """
        Extract entities for a specific zone

        Args:
            lake_id: Lake ID
            zone_id: Zone ID

        Returns:
            List of entity dictionaries
        """
        entities_data = []

        try:
            entities_parent = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}/zones/{zone_id}"
            entities_request = dataplex_v1.ListEntitiesRequest(parent=entities_parent)
            entities = self.metadata_client.list_entities(request=entities_request)

            for entity in entities:
                # Fetch the full entity with schema information
                entity_full = self._extract_entity_full_details(
                    entity, lake_id, zone_id
                )

                # Determine platform from asset
                platform = self._determine_platform_from_asset(
                    entity_full, lake_id, zone_id
                )

                # Build fully qualified name for lineage lookup
                fully_qualified_name = f"{platform}:{entity_full.id}"
                if entity_full.data_path:
                    fully_qualified_name = f"{platform}:{entity_full.data_path}"

                # Extract lineage for this entity
                lineage = self.extract_lineage_for_entity(fully_qualified_name)

                # Extract schema fields
                schema_fields = self._extract_schema_fields(entity_full)

                # Extract aspects (custom metadata)
                aspects = self._extract_aspects(entity_full, "entity")

                entity_data = {
                    "name": entity_full.name,
                    "id": entity_full.id,
                    "lake_id": lake_id,
                    "zone_id": zone_id,
                    "asset": entity_full.asset,
                    "type": entity_full.type_.name,
                    "platform": platform,
                    "data_path": entity_full.data_path,
                    "create_time": str(entity_full.create_time),
                    "schema_fields": schema_fields,
                    "aspects": aspects,
                    "datahub_urn": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{entity_full.id},PROD)",
                    "lineage_upstream": lineage["upstream"],
                    "lineage_downstream": lineage["downstream"],
                }
                entities_data.append(entity_data)

                # Log lineage info if found
                lineage_info = ""
                if lineage["upstream"] or lineage["downstream"]:
                    lineage_info = f" (upstream: {len(lineage['upstream'])}, downstream: {len(lineage['downstream'])})"
                schema_info = f", {len(schema_fields)} fields" if schema_fields else ""
                aspect_info = f", {len(aspects)} aspects" if aspects else ""
                logger.info(
                    f"      Found entity: {entity_full.id}{lineage_info}{schema_info}{aspect_info}"
                )
        except exceptions.GoogleAPICallError as e:
            logger.error(f"      Error listing entities: {e}")

        return entities_data

    def _extract_entries_for_group(self, entry_group_id: str) -> List[Dict[str, Any]]:
        """
        Extract entries for a specific entry group

        Args:
            entry_group_id: Entry group ID

        Returns:
            List of entry dictionaries
        """
        entries_data = []

        try:
            entries_parent = f"projects/{self.project_id}/locations/{self.location}/entryGroups/{entry_group_id}"
            entries_request = dataplex_v1.ListEntriesRequest(parent=entries_parent)
            entries = self.catalog_client.list_entries(request=entries_request)

            for entry in entries:
                entry_id = entry.name.split("/")[-1]

                # Fetch full entry with aspects using EntryView.ALL
                try:
                    get_entry_request = dataplex_v1.GetEntryRequest(
                        name=entry.name, view=dataplex_v1.EntryView.ALL
                    )
                    entry_full = self.catalog_client.get_entry(
                        request=get_entry_request
                    )
                except exceptions.GoogleAPICallError as e:
                    logger.warning(
                        f"      Could not fetch full entry details for {entry_id}: {e}"
                    )
                    entry_full = entry

                # Extract aspects from entry
                entry_aspects = self._extract_aspects(entry_full, "entry")

                entry_data = {
                    "name": entry_full.name,
                    "id": entry_id,
                    "entry_group_id": entry_group_id,
                    "entry_type": entry_full.entry_type,
                    "create_time": str(entry_full.create_time),
                    "fully_qualified_name": entry_full.fully_qualified_name
                    if entry_full.fully_qualified_name
                    else "",
                    "aspects": entry_aspects,
                    "datahub_urn": f"urn:li:container:{entry_id}",
                }
                entries_data.append(entry_data)

                aspect_count_info = (
                    f", {len(entry_aspects)} aspects" if entry_aspects else ""
                )
                logger.info(f"    Found entry: {entry_id}{aspect_count_info}")
        except exceptions.GoogleAPICallError as e:
            logger.error(f"    Error listing entries: {e}")

        return entries_data

    def _extract_data_scans(
        self,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract data scans (quality and profiling) and their results

        Returns:
            Tuple of (data_scans, quality_results, profile_results)
        """
        data_scans = []
        quality_results = []
        profile_results = []

        try:
            parent = f"projects/{self.project_id}/locations/{self.location}"
            request = dataplex_v1.ListDataScansRequest(parent=parent)
            scans = self.datascan_client.list_data_scans(request=request)

            for scan in scans:
                scan_id = scan.name.split("/")[-1]
                scan_type = "Unknown"

                if scan.data_quality_spec:
                    scan_type = "Data Quality"
                elif scan.data_profile_spec:
                    scan_type = "Data Profiling"

                scan_data = {
                    "name": scan.name,
                    "id": scan_id,
                    "display_name": scan.display_name,
                    "type": scan_type,
                    "state": scan.state.name,
                    "create_time": str(scan.create_time),
                    "description": scan.description or "",
                    "data_resource": scan.data.resource if scan.data else "",
                    "data_entity": scan.data.entity
                    if (scan.data and scan.data.entity)
                    else "",
                    "datahub_urn": f"urn:li:assertion:{scan_id}",
                }

                # Add data quality specific information
                if scan.data_quality_spec:
                    scan_data["sampling_percent"] = (
                        scan.data_quality_spec.sampling_percent or 100
                    )
                    scan_data["rule_count"] = (
                        len(scan.data_quality_spec.rules)
                        if scan.data_quality_spec.rules
                        else 0
                    )
                    scan_data["rules"] = self._extract_quality_rules(
                        scan.data_quality_spec
                    )

                # Add data profiling specific information
                if scan.data_profile_spec:
                    scan_data["sampling_percent"] = (
                        scan.data_profile_spec.sampling_percent or 100
                    )
                    scan_data["row_filter"] = (
                        scan.data_profile_spec.row_filter
                        if scan.data_profile_spec.row_filter
                        else ""
                    )

                data_scans.append(scan_data)
                logger.info(f"  Found data scan: {scan_id} ({scan_type})")

                # Extract latest job results
                quality_result, profile_result = self._extract_scan_job_results(
                    scan_id, scan.name
                )
                if quality_result:
                    quality_results.append(quality_result)
                if profile_result:
                    profile_results.append(profile_result)

        except exceptions.GoogleAPICallError as e:
            logger.error(f"  Error listing data scans: {e}")

        return data_scans, quality_results, profile_results

    def _extract_quality_rules(self, quality_spec: Any) -> List[Dict[str, Any]]:
        """Extract quality rules from a data quality spec"""
        rules = []
        if not quality_spec.rules:
            return rules

        for rule in quality_spec.rules:
            rule_type = "Unknown"
            if rule.non_null_expectation:
                rule_type = "Non-Null"
            elif rule.range_expectation:
                rule_type = "Range"
            elif rule.regex_expectation:
                rule_type = "Regex"
            elif rule.set_expectation:
                rule_type = "Set"
            elif rule.uniqueness_expectation:
                rule_type = "Uniqueness"
            elif rule.statistic_range_expectation:
                rule_type = "Statistic Range"
            elif rule.row_condition_expectation:
                rule_type = "Row Condition"
            elif rule.table_condition_expectation:
                rule_type = "Table Condition"
            elif rule.sql_assertion:
                rule_type = "SQL Assertion"

            rules.append(
                {
                    "dimension": rule.dimension or "N/A",
                    "threshold": rule.threshold or 0.0,
                    "type": rule_type,
                    "column": rule.column if hasattr(rule, "column") else "",
                }
            )
        return rules

    def _extract_scan_job_results(
        self, scan_id: str, scan_name: str
    ) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
        """
        Extract latest job results for a scan

        Returns:
            Tuple of (quality_result, profile_result) - either can be None
        """
        quality_result = None
        profile_result = None

        try:
            jobs_parent = f"projects/{self.project_id}/locations/{self.location}/dataScans/{scan_id}"
            jobs_request = dataplex_v1.ListDataScanJobsRequest(
                parent=jobs_parent, page_size=LATEST_JOB_PAGE_SIZE
            )
            jobs = self.datascan_client.list_data_scan_jobs(request=jobs_request)

            for job in jobs:
                job_id = job.name.split("/")[-1]

                if job.data_quality_result:
                    quality_result = {
                        "scan_id": scan_id,
                        "scan_name": scan_name,
                        "job_id": job_id,
                        "job_name": job.name,
                        "state": job.state.name,
                        "start_time": str(job.start_time) if job.start_time else "",
                        "end_time": str(job.end_time) if job.end_time else "",
                        "passed": job.data_quality_result.passed,
                        "score": job.data_quality_result.score
                        if hasattr(job.data_quality_result, "score")
                        else None,
                        "dimensions_count": len(job.data_quality_result.dimensions)
                        if job.data_quality_result.dimensions
                        else 0,
                        "rules_evaluated": len(job.data_quality_result.rules)
                        if job.data_quality_result.rules
                        else 0,
                        "rules_passed": sum(
                            1 for r in job.data_quality_result.rules if r.passed
                        )
                        if job.data_quality_result.rules
                        else 0,
                    }
                    logger.info(
                        f"    Found quality result: Job {job_id} - Passed: {job.data_quality_result.passed}"
                    )

                if job.data_profile_result:
                    profile_result = {
                        "scan_id": scan_id,
                        "scan_name": scan_name,
                        "job_id": job_id,
                        "job_name": job.name,
                        "state": job.state.name,
                        "start_time": str(job.start_time) if job.start_time else "",
                        "end_time": str(job.end_time) if job.end_time else "",
                        "row_count": job.data_profile_result.row_count,
                        "fields_profiled": len(job.data_profile_result.profile.fields)
                        if (
                            job.data_profile_result.profile
                            and job.data_profile_result.profile.fields
                        )
                        else 0,
                    }
                    logger.info(
                        f"    Found profile result: Job {job_id} - Row Count: {job.data_profile_result.row_count}"
                    )

                break  # Only process the first (most recent) job

        except exceptions.GoogleAPICallError as e:
            logger.error(f"    Error listing jobs for scan {scan_id}: {e}")

        return quality_result, profile_result

    def _match_entities_with_entries(
        self, entities: List[Dict], entries: List[Dict]
    ) -> List[Dict]:
        """
        Match entities with their corresponding catalog entries based on fully qualified name

        Args:
            entities: List of entity dictionaries
            entries: List of entry dictionaries

        Returns:
            List of entity dictionaries with 'catalog_entry' field added
        """
        # Create a lookup map of entries by their fully_qualified_name
        entries_by_fqn = {}
        for entry in entries:
            fqn = entry.get("fully_qualified_name", "")
            if fqn:
                entries_by_fqn[fqn] = entry

        # Match entities with entries
        merged_entities = []
        for entity in entities:
            # Construct FQN for entity (same format used during extraction)
            entity_fqn = f"{entity['platform']}:{entity['id']}"

            # Alternative FQN using data_path if available (convert to dot notation)
            entity_fqn_alt = None
            if entity.get("data_path"):
                # Parse data_path: projects/PROJECT/datasets/DATASET/tables/TABLE
                # Convert to: platform:PROJECT.DATASET.TABLE
                data_path = entity["data_path"]
                parts = data_path.split("/")
                if (
                    len(parts) >= 6
                    and parts[0] == "projects"
                    and parts[2] == "datasets"
                    and parts[4] == "tables"
                ):
                    project = parts[1]
                    dataset = parts[3]
                    table = parts[5]
                    entity_fqn_alt = f"{entity['platform']}:{project}.{dataset}.{table}"

            # Try to find matching entry
            matched_entry = entries_by_fqn.get(entity_fqn)
            if not matched_entry and entity_fqn_alt:
                matched_entry = entries_by_fqn.get(entity_fqn_alt)

            # Add catalog entry info to entity
            entity_copy = entity.copy()
            if matched_entry:
                entity_copy["catalog_entry"] = {
                    "entry_id": matched_entry["id"],
                    "entry_name": matched_entry["name"],
                    "entry_group_id": matched_entry["entry_group_id"],
                    "entry_type": matched_entry["entry_type"],
                    "entry_aspects": matched_entry.get("aspects", {}),
                    "create_time": matched_entry["create_time"],
                }
                logger.info(
                    f"    ✓ Matched entity '{entity['id']}' with entry '{matched_entry['id']}'"
                )
            else:
                entity_copy["catalog_entry"] = None
                logger.warning(
                    f"    ✗ No catalog entry found for entity '{entity['id']}' (tried FQNs: '{entity_fqn}', '{entity_fqn_alt}')"
                )

            merged_entities.append(entity_copy)

        return merged_entities

    def extract_all_data(self) -> Dict[str, Any]:
        """Extract all Dataplex data and return as structured dictionary"""
        extraction_data = {
            "extraction_time": datetime.now().isoformat(),
            "project_id": self.project_id,
            "location": self.location,
            "lakes": [],
            "zones": [],
            "assets": [],
            "entities": [],
            "entry_groups": [],
            "entries": [],
            "data_scans": [],
            "data_quality_results": [],
            "data_profile_results": [],
        }

        logger.info("Starting data extraction...")

        # Extract Lakes
        try:
            parent = f"projects/{self.project_id}/locations/{self.location}"
            request = dataplex_v1.ListLakesRequest(parent=parent)
            lakes = self.dataplex_client.list_lakes(request=request)

            for lake in lakes:
                lake_id = lake.name.split("/")[-1]
                lake_data = {
                    "name": lake.name,
                    "id": lake_id,
                    "display_name": lake.display_name,
                    "state": lake.state.name,
                    "create_time": str(lake.create_time),
                    "description": lake.description or "",
                    "datahub_urn": f"urn:li:domain:{lake_id}",
                }
                extraction_data["lakes"].append(lake_data)
                logger.info(f"  Found lake: {lake_id}")

                # Extract Zones for this lake
                try:
                    zones_parent = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}"
                    zones_request = dataplex_v1.ListZonesRequest(parent=zones_parent)
                    zones = self.dataplex_client.list_zones(request=zones_request)

                    for zone in zones:
                        zone_id = zone.name.split("/")[-1]
                        zone_data = {
                            "name": zone.name,
                            "id": zone_id,
                            "lake_id": lake_id,
                            "display_name": zone.display_name,
                            "type": zone.type_.name,
                            "state": zone.state.name,
                            "create_time": str(zone.create_time),
                            "description": zone.description or "",
                            "datahub_urn": f"urn:li:domain:{zone_id}",
                        }
                        extraction_data["zones"].append(zone_data)
                        logger.info(f"    Found zone: {zone_id}")

                        # Extract Assets for this zone
                        try:
                            assets_parent = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}/zones/{zone_id}"
                            assets_request = dataplex_v1.ListAssetsRequest(
                                parent=assets_parent
                            )
                            assets = self.dataplex_client.list_assets(
                                request=assets_request
                            )

                            for asset in assets:
                                asset_id = asset.name.split("/")[-1]
                                asset_data = {
                                    "name": asset.name,
                                    "id": asset_id,
                                    "lake_id": lake_id,
                                    "zone_id": zone_id,
                                    "display_name": asset.display_name,
                                    "state": asset.state.name,
                                    "create_time": str(asset.create_time),
                                    "description": asset.description or "",
                                    "resource_type": asset.resource_spec.type_.name
                                    if asset.resource_spec
                                    else "",
                                    "resource_name": asset.resource_spec.name
                                    if asset.resource_spec
                                    else "",
                                    "datahub_urn": f"urn:li:dataProduct:{asset_id}",
                                }
                                extraction_data["assets"].append(asset_data)
                                logger.info(f"      Found asset: {asset_id}")
                        except exceptions.GoogleAPICallError as e:
                            logger.error(f"      Error listing assets: {e}")

                        # Extract Entities for this zone
                        entities_data = self._extract_entities_for_zone(
                            lake_id, zone_id
                        )
                        extraction_data["entities"].extend(entities_data)

                except exceptions.GoogleAPICallError as e:
                    logger.error(f"    Error listing zones: {e}")

        except exceptions.GoogleAPICallError as e:
            logger.error(f"  Error listing lakes: {e}")

        # Extract Entry Groups
        try:
            parent = f"projects/{self.project_id}/locations/{self.location}"
            request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
            entry_groups = self.catalog_client.list_entry_groups(request=request)

            for group in entry_groups:
                entry_group_id = group.name.split("/")[-1]
                group_data = {
                    "name": group.name,
                    "id": entry_group_id,
                    "display_name": group.display_name,
                    "description": group.description or "",
                    "create_time": str(group.create_time),
                    "datahub_urn": f"urn:li:container:{entry_group_id}",
                }
                extraction_data["entry_groups"].append(group_data)
                logger.info(f"  Found entry group: {entry_group_id}")

                # Extract Entries for this entry group
                entries_data = self._extract_entries_for_group(entry_group_id)
                extraction_data["entries"].extend(entries_data)

        except exceptions.GoogleAPICallError as e:
            logger.error(f"  Error listing entry groups: {e}")

        # Extract Data Scans (Data Quality and Data Profiling)
        data_scans, quality_results, profile_results = self._extract_data_scans()
        extraction_data["data_scans"].extend(data_scans)
        extraction_data["data_quality_results"].extend(quality_results)
        extraction_data["data_profile_results"].extend(profile_results)

        # Match entities with catalog entries
        logger.info("Matching entities with catalog entries...")
        extraction_data["entities"] = self._match_entities_with_entries(
            extraction_data["entities"], extraction_data["entries"]
        )

        matched_count = sum(
            1 for e in extraction_data["entities"] if e.get("catalog_entry")
        )
        logger.info(
            f"  Matched {matched_count} out of {len(extraction_data['entities'])} entities with catalog entries"
        )

        logger.info("Data extraction complete!")
        return extraction_data
