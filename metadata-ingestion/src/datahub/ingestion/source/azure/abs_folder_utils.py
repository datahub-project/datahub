import logging
from typing import Dict, Iterable, List, Optional

from azure.storage.blob import BlobProperties

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


def get_abs_properties(
    container_name: str,
    blob_name: Optional[str],
    full_path: str,
    number_of_files: int,
    size_in_bytes: int,
    sample_files: bool,
    azure_config: Optional[AzureConnectionConfig],
    use_abs_container_properties: Optional[bool] = False,
    use_abs_blob_properties: Optional[bool] = False,
) -> Dict[str, str]:
    if azure_config is None:
        raise ValueError(
            "Azure configuration is not provided. Cannot retrieve container client."
        )

    blob_service_client = azure_config.get_blob_service_client()
    container_client = blob_service_client.get_container_client(
        container=container_name
    )

    custom_properties = {"schema_inferred_from": full_path}
    if not sample_files:
        custom_properties.update(
            {
                "number_of_files": str(number_of_files),
                "size_in_bytes": str(size_in_bytes),
            }
        )

    if use_abs_blob_properties and blob_name is not None:
        blob_client = container_client.get_blob_client(blob=blob_name)
        blob_properties = blob_client.get_blob_properties()
        if blob_properties:
            create_properties(
                data=blob_properties,
                prefix="blob",
                custom_properties=custom_properties,
                resource_name=blob_name,
                json_properties=[
                    "metadata",
                    "content_settings",
                    "lease",
                    "copy",
                    "immutability_policy",
                ],
            )
        else:
            logger.warning(
                f"No blob properties found for container={container_name}, blob={blob_name}."
            )

    if use_abs_container_properties:
        container_properties = container_client.get_container_properties()
        if container_properties:
            create_properties(
                data=container_properties,
                prefix="container",
                custom_properties=custom_properties,
                resource_name=container_name,
                json_properties=["metadata"],
            )
        else:
            logger.warning(
                f"No container properties found for container={container_name}."
            )

    return custom_properties


def add_property(
    key: str, value: str, custom_properties: Dict[str, str], resource_name: str
) -> Dict[str, str]:
    if key in custom_properties:
        key = f"{key}_{resource_name}"
    if value is not None:
        custom_properties[key] = str(value)
    return custom_properties


def create_properties(
    data: BlobProperties,
    prefix: str,
    custom_properties: Dict[str, str],
    resource_name: str,
    json_properties: List[str],
) -> None:
    for item in data.items():
        key = item[0]
        transformed_key = f"{prefix}_{key}"
        value = item[1]
        if value is None:
            continue
        try:
            # These are known properties with a json value, we process these recursively...
            if key in json_properties:
                create_properties(
                    data=value,
                    prefix=f"{prefix}_{key}",
                    custom_properties=custom_properties,
                    resource_name=resource_name,
                    json_properties=json_properties,
                )
            else:
                custom_properties = add_property(
                    key=transformed_key,
                    value=value,
                    custom_properties=custom_properties,
                    resource_name=resource_name,
                )
        except Exception as exception:
            logger.debug(
                f"Could not create property {key} value {value}, from resource {resource_name}: {exception}."
            )


def get_abs_tags(
    container_name: str,
    key_name: Optional[str],
    dataset_urn: str,
    azure_config: Optional[AzureConnectionConfig],
    ctx: PipelineContext,
    use_abs_blob_tags: Optional[bool] = False,
) -> Optional[GlobalTagsClass]:
    # Todo add the service_client, when building out this get_abs_tags
    if azure_config is None:
        raise ValueError(
            "Azure configuration is not provided. Cannot retrieve container client."
        )

    tags_to_add: List[str] = []
    blob_service_client = azure_config.get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob=key_name)

    if use_abs_blob_tags and key_name is not None:
        tag_set = blob_client.get_blob_tags()
        if tag_set:
            tags_to_add.extend(
                make_tag_urn(f"""{key}:{value}""") for key, value in tag_set.items()
            )
        else:
            # Unlike container tags, if an object does not have tags, it will just return an empty array
            # as opposed to an exception.
            logger.info(f"No tags found for container={container_name} key={key_name}")

    if len(tags_to_add) == 0:
        return None

    if ctx.graph is not None:
        logger.debug("Connected to DatahubApi, grabbing current tags to maintain.")
        current_tags: Optional[GlobalTagsClass] = ctx.graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=GlobalTagsClass,
        )
        if current_tags:
            tags_to_add.extend([current_tag.tag for current_tag in current_tags.tags])
    else:
        logger.warning("Could not connect to DatahubApi. No current tags to maintain")

    # Sort existing tags
    tags_to_add = sorted(list(set(tags_to_add)))
    # Remove duplicate tags
    new_tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
    )
    return new_tags


def list_folders(
    container_name: str, prefix: str, azure_config: Optional[AzureConnectionConfig]
) -> Iterable[str]:
    if azure_config is None:
        raise ValueError(
            "Azure configuration is not provided. Cannot retrieve container client."
        )

    abs_blob_service_client = azure_config.get_blob_service_client()
    container_client = abs_blob_service_client.get_container_client(container_name)

    current_level = prefix.count("/")

    # Use a larger page size for better performance
    blob_list = container_client.list_blobs(
        name_starts_with=prefix,
        results_per_page=5000,  # Increased from default for better performance
    )

    # Use a set for faster lookups
    unique_folders: set[str] = set()

    # Process blobs in batches for better memory usage
    batch_size = 1000
    batch = []

    for blob in blob_list:
        batch.append(blob)

        if len(batch) >= batch_size:
            _process_folder_batch(batch, prefix, current_level, unique_folders)
            batch = []

    # Process any remaining blobs
    if batch:
        _process_folder_batch(batch, prefix, current_level, unique_folders)

    # Return the unique folders
    yield from sorted(unique_folders)


def _process_folder_batch(blobs, prefix, current_level, unique_folders):
    """Process a batch of blobs to extract folder names efficiently"""
    for blob in blobs:
        blob_name = blob.name[: blob.name.rfind("/") + 1]
        folder_structure_arr = blob_name.split("/")

        if len(folder_structure_arr) <= current_level:
            continue

        folder_name = folder_structure_arr[current_level]
        if not folder_name:
            continue

        folder_path = f"{prefix}{folder_name}"
        unique_folders.add(folder_path)
