"""Configuration for the DataHub Entity Embeddings source."""

from typing import Dict, List, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
    IncrementalConfig,
    LockConfig,
)
from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
    DocumentChunkingStatefulIngestionConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DataHubConnectionConfig,
    EmbeddingConfig,
)

AUTO_ENTITY_TYPES = "auto"


class EntityTextConfig(ConfigModel):
    """How searchable metadata is rendered into the text that gets embedded.

    Everything here is data: which fields exist for each entity type comes from the
    server's entity registry, these settings only tune labels, limits and merges.
    """

    include_custom_properties: bool = Field(
        default=False,
        description="Include customProperties maps. Off by default: ingestion sources "
        "fill them with technical key/values that add noise and embedding cost.",
    )
    reference_entity_types: List[str] = Field(
        default=[
            "tag",
            "glossaryTerm",
            "glossaryNode",
            "domain",
            "container",
            "dataPlatform",
            "dataProduct",
            "application",
        ],
        description="Referenced entities (URN fields) rendered by display name. Other "
        "references (owners, lineage, other assets) are left out of the text.",
    )
    reference_id_fallback_types: List[str] = Field(
        default=["tag", "dataPlatform"],
        description="Reference types whose URN id is a readable name, used when the "
        "referenced entity has no name aspect.",
    )
    item_key_fields: List[str] = Field(
        default=["fieldPath", "name", "id"],
        description="Fields that identify an item of an array of records (e.g. a column).",
    )
    merge_array_groups: Dict[str, str] = Field(
        default={
            "editableSchemaMetadata.editableSchemaFieldInfo": "schemaMetadata.fields",
        },
        description="Render the items of an array into the items of another one, "
        "matched by item key (e.g. UI-edited column descriptions into the schema columns).",
    )
    group_labels: Dict[str, str] = Field(
        default={
            "schemaMetadata.fields": "Columns",
            "dataJobInputSchemaMetadata.fields": "Input columns",
            "dataJobOutputSchemaMetadata.fields": "Output columns",
            "inputFields.fields": "Input fields",
        },
        description="Section title per '<aspect>.<array path>'. Defaults to the array name.",
    )
    field_labels: Dict[str, str] = Field(
        default={
            "id": "Identifier",
            "description": "Description",
            "editedDescription": "Description",
            "definition": "Description",
            "fieldDescriptions": "Description",
            "editedFieldDescriptions": "Description",
            "fieldLabels": "Label",
            "fieldTags": "Tags",
            "editedFieldTags": "Tags",
            "fieldGlossaryTerms": "Glossary terms",
            "editedFieldGlossaryTerms": "Glossary terms",
            "glossaryTerms": "Glossary terms",
            "domains": "Domain",
            "platform": "Platform",
            "platformInstance": "Platform instance",
        },
        description="Label per searchable fieldName. Defaults to the humanized fieldName.",
    )
    unlabelled_item_fields: List[str] = Field(
        default=["Description"],
        description="Item field labels rendered without a 'Label:' prefix.",
    )
    entity_type_labels: Dict[str, str] = Field(
        default={
            "corpuser": "User",
            "corpGroup": "Group",
            "mlModel": "ML model",
            "mlModelGroup": "ML model group",
            "mlFeature": "ML feature",
            "mlFeatureTable": "ML feature table",
            "mlPrimaryKey": "ML primary key",
            "dataHubRole": "Role",
            "aiAgent": "AI agent",
            "api": "API",
        },
        description="Title prefix per entity type. Defaults to the humanized type name.",
    )
    exclude_value_patterns: List[str] = Field(
        default=[r"\d{4}-\d{2}-\d{2}(T|_{1,2}| )\d{2}[:_]\d{2}"],
        description="Regexes of referenced names (tags, terms, ...) left out of the "
        "text. The default drops names that embed a timestamp (e.g. dbt 'last_updated:2026-09-28__00_02_26' tags): they "
        "change on every run and would re-embed the entity each time.",
    )
    include_siblings: bool = Field(
        default=True,
        description="Merge the text of sibling entities (e.g. a dbt model and its "
        "BigQuery table) so each sibling is findable by the other's documentation. "
        "Siblings are the same asset, so their text is merged whatever their platform; "
        "platform_pattern only decides which entities get embeddings.",
    )
    inline_value_chars: int = Field(
        default=200,
        gt=0,
        description="Values up to this length are rendered inline as 'Label: value'; "
        "longer ones get their own section.",
    )
    max_value_chars: int = Field(default=2000, gt=0)
    max_items_per_group: int = Field(default=150, gt=0)
    max_text_chars: int = Field(
        default=24000,
        gt=0,
        description="Upper bound of the embedded text; with the default chunking it caps "
        "each entity at about 6 chunks, which bounds storage per entity.",
    )


class DataHubEntityEmbeddingsSourceConfig(
    StatefulIngestionConfigBase[DocumentChunkingStatefulIngestionConfig]
):
    """Configuration for the DataHub Entity Embeddings source."""

    datahub: DataHubConnectionConfig = Field(
        default_factory=DataHubConnectionConfig,
        description="DataHub connection, only used when the pipeline has no graph "
        "(managed ingestion and datahub-rest sinks provide one).",
    )
    entity_types: List[str] = Field(
        default=[AUTO_ENTITY_TYPES],
        description="Entity types to embed. 'auto' means every type the server has "
        "enabled for semantic search (appConfig.semanticSearchConfig.enabledEntities), "
        "so adding a type only requires enabling it on the server. Explicit types must "
        "also be enabled on the server.",
    )
    exclude_entity_types: List[str] = Field(
        default=[],
        description="Entity types never embedded by this source.",
    )
    search_groups: List[str] = Field(
        default=["primary"],
        description="Registry search groups eligible for embedding, for registries that "
        "assign entity types to search groups. 'primary' covers the catalog's "
        "user-facing assets; operational groups ('timeseries', 'query', 'schemaField', "
        "'default') can hold millions of entities and must be opted into. Entity types "
        "without a search group are not filtered by this setting.",
    )
    platform_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Platforms whose entities are embedded, matched against the platform "
        "name (e.g. 'pubsub', 'bigquery') of every entity type that has one: the "
        "dataPlatformInstance aspect, else the platform in the entity's URN. Entities "
        "without a platform (tags, domains, glossary terms...) are not affected. It "
        "applies to future runs only: entities embedded before they were excluded keep "
        "their semanticContent.",
    )
    text: EntityTextConfig = Field(default_factory=EntityTextConfig)
    chunking: ChunkingConfig = Field(
        default_factory=lambda: ChunkingConfig(
            strategy="by_title", max_characters=4000, combine_text_under_n_chars=500
        ),
    )
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)
    locking: LockConfig = Field(default_factory=LockConfig)
    scroll_batch_size: int = Field(
        default=100,
        gt=0,
        le=1000,
        description="Entities (with their aspects) fetched per scroll page.",
    )
    max_entities_per_run: int = Field(
        default=0,
        ge=-1,
        description="Stop cleanly after embedding this many entities; with stateful "
        "ingestion, later runs continue where this one stopped. 0 or -1 disables the "
        "limit.",
    )
    time_budget_seconds: Optional[int] = Field(
        default=None,
        gt=0,
        description="Stop cleanly once this much time has passed so the incremental "
        "state is committed before the job's deadline; with stateful ingestion, "
        "later runs continue.",
    )
    max_consecutive_failures: int = Field(
        default=25,
        gt=0,
        description="Abort the run after this many consecutive embedding failures "
        "(e.g. the provider is down) instead of failing every remaining entity.",
    )
    index_delay_seconds: float = Field(
        default=0.0,
        ge=0,
        description="Pause after each embedded entity to smooth write load on GMS.",
    )
    stateful_ingestion: DocumentChunkingStatefulIngestionConfig = Field(
        default_factory=lambda: DocumentChunkingStatefulIngestionConfig(enabled=True),
        description="Stateful ingestion keeps the text hash of every embedded entity "
        "so unchanged entities are not re-embedded.",
    )

    @field_validator("entity_types")
    @classmethod
    def _entity_types_not_empty(cls, v: List[str]) -> List[str]:
        if not any(t.strip() for t in v):
            raise ValueError(
                f"entity_types must list '{AUTO_ENTITY_TYPES}' or at least one entity "
                "type; an empty list would embed nothing."
            )
        return v
