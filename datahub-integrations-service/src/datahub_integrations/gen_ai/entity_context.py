from pprint import pp

from datahub.metadata.schema_classes import DictWrapper

from datahub_integrations.app import graph

VIEW_LOGIC_MAX_LENGTH = 500
SCHEMA_METADATA_MAX_LENGTH = 1000


def _truncate_to_len(s: str, max_len: int) -> str:
    if len(s) > max_len:
        s = s[:max_len] + " [truncated]"
    return s


def _generate_context_single(entity_urn: str) -> dict:
    entity = graph.get_entity_semityped(entity_urn)

    # Reduce the entity to just the fields we need.
    IRRELEVANT_ASPECTS = [
        "browsePaths",
        "test",
        "container",
        "schemaProposals",
        "datasetKey",
        "dataPlatformInstance",
        "testResults",
        "status",
    ]

    for aspect in IRRELEVANT_ASPECTS:
        entity.pop(aspect, None)  # type: ignore
    if "upstreamLineage" in entity:
        entity["upstreamLineage"].fineGrainedLineages = None

    if "schemaMetadata" in entity:
        fields = entity["schemaMetadata"].fields
        ddl = None

        # Proper DDL statements tend to work better with LLMs.
        if fields:
            ddl = "CREATE TABLE (\n"
            for field in fields:
                if field.description:
                    ddl += f"  {field.fieldPath} {field.nativeDataType} COMMENT '{field.description}',\n"
                else:
                    ddl += f"  {field.fieldPath} {field.nativeDataType},\n"
            ddl += ")"

            ddl = _truncate_to_len(ddl, SCHEMA_METADATA_MAX_LENGTH)

        entity["schemaMetadata"] = ddl  # type: ignore

    # Truncate view properties SQL to 500 chars.
    if "viewProperties" in entity:
        entity["viewProperties"].viewLogic = _truncate_to_len(
            entity["viewProperties"].viewLogic, VIEW_LOGIC_MAX_LENGTH
        )

    # TODO resolve domain
    # TODO simplify ownership

    entity_json = {}
    for key, value in entity.items():
        if value:
            if isinstance(value, DictWrapper):
                value = value.to_obj()
            entity_json[key] = value

    return entity_json


def generate_context(entity_urn: str) -> dict:
    """Generate a context for the entity."""

    entity_context = _generate_context_single(entity_urn)

    if "siblings" in entity_context:
        # If this is a siblings node, we need to get both this and the primary.
        siblings = entity_context.pop("siblings")

        sibling_urn = siblings["siblings"][0]
        sibling_context = _generate_context_single(sibling_urn)
        sibling_context.pop("siblings", None)

        # TODO: Merge these more intelligently. However, this seems to work
        # well enough for now.
        entity_context = {
            "primary": entity_context,
            "dbt": sibling_context,
        }

    pp(entity_context)
    return entity_context
