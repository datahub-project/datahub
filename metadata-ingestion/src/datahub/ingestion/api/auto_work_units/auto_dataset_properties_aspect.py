import dataclasses
import json
from typing import Dict, Iterable, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    OperationClass,
    TimeStampClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import guess_entity_type


@dataclasses.dataclass
class TimestampPair:
    last_modified_dataset_props: Optional[
        TimeStampClass
    ]  # last_modified of datasetProperties aspect
    last_updated_timestamp_dataset_props: Optional[
        int
    ]  # lastUpdatedTimestamp of the operation aspect


def try_aspect_from_metadata_change_proposal_class(
    wu: MetadataWorkUnit,
) -> Optional[DatasetPropertiesClass]:
    if (
        isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "datasetProperties"
        and wu.metadata.changeType == ChangeTypeClass.PATCH
        and isinstance(wu.metadata.aspect, GenericAspectClass)
    ):
        patch_dataset_properties = json.loads(wu.metadata.aspect.value)
        for operation in patch_dataset_properties:
            if operation.get("path") == "/lastModified":
                # Deserializing `lastModified` as the `auto_patch_last_modified` function relies on this property
                # to decide if a patch aspect for the datasetProperties aspect should be generated
                return DatasetPropertiesClass(
                    lastModified=TimeStampClass(time=operation["value"]["time"])
                )

    return None


def auto_patch_last_modified(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """
    Generate a patch request for datasetProperties aspect in-case
        1. `lastModified` of datasetProperties is not set
        2. And there are operation aspects
    in this case set the `lastModified` of datasetProperties to max value of operation aspects `lastUpdatedTimestamp`.

    We need this functionality to support sort by `last modified` on UI.
    """
    candidate_dataset_for_patch: Dict[str, TimestampPair] = {}

    for wu in stream:
        if (
            guess_entity_type(wu.get_urn()) != "dataset"
        ):  # we are only processing datasets
            yield wu
            continue

        dataset_properties_aspect = wu.get_aspect_of_type(
            DatasetPropertiesClass
        ) or try_aspect_from_metadata_change_proposal_class(wu)
        dataset_operation_aspect = wu.get_aspect_of_type(OperationClass)

        timestamp_pair = candidate_dataset_for_patch.get(wu.get_urn())

        if timestamp_pair:
            # Update the timestamp_pair
            if dataset_properties_aspect and dataset_properties_aspect.lastModified:
                timestamp_pair.last_modified_dataset_props = (
                    dataset_properties_aspect.lastModified
                )

            if (
                dataset_operation_aspect
                and dataset_operation_aspect.lastUpdatedTimestamp
            ):
                timestamp_pair.last_updated_timestamp_dataset_props = max(
                    timestamp_pair.last_updated_timestamp_dataset_props or 0,
                    dataset_operation_aspect.lastUpdatedTimestamp,
                )

        else:
            # Create new TimestampPair
            last_modified_dataset_props: Optional[TimeStampClass] = None
            last_updated_timestamp_dataset_props: Optional[int] = None

            if dataset_properties_aspect:
                last_modified_dataset_props = dataset_properties_aspect.lastModified

            if dataset_operation_aspect:
                last_updated_timestamp_dataset_props = (
                    dataset_operation_aspect.lastUpdatedTimestamp
                )

            candidate_dataset_for_patch[wu.get_urn()] = TimestampPair(
                last_modified_dataset_props=last_modified_dataset_props,
                last_updated_timestamp_dataset_props=last_updated_timestamp_dataset_props,
            )

        yield wu

    # Emit a patch datasetProperties aspect for dataset where last_modified is None
    for entity_urn, timestamp_pair in candidate_dataset_for_patch.items():
        # Emit patch if last_modified is not set and last_updated_timestamp is set
        if (
            timestamp_pair.last_modified_dataset_props is None
            and timestamp_pair.last_updated_timestamp_dataset_props
        ):
            dataset_patch_builder = DatasetPatchBuilder(urn=entity_urn)

            dataset_patch_builder.set_last_modified(
                timestamp=TimeStampClass(
                    time=timestamp_pair.last_updated_timestamp_dataset_props
                )
            )

            yield from [
                MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp),
                    mcp_raw=mcp,
                )
                for mcp in dataset_patch_builder.build()
            ]
