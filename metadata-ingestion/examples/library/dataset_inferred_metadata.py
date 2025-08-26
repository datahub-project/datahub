# Imports for urn construction utility methods
from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_tag_urn,
    make_term_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    InferredDescriptionClass,
    InferredGlossaryTermsClass,
    InferredMetadataClass,
    InferredMetadataSourceClass,
    InferredNeighborClass,
    InferredNeighborsClass,
    InferredOwnersClass,
    InferredTagsClass,
    OwnerClass,
    OwnershipTypeClass,
    SimilarityFactorClass,
)

ineighbors = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn("snowflake", "table1"),
    aspect=InferredNeighborsClass(
        inferredNeighbors=[
            InferredNeighborClass(
                urn=make_dataset_urn("dbt", "table1"),
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityAPI v1",
                    score=0.88,
                    similarityFactors=[
                        SimilarityFactorClass.DATASET_SCHEMA,
                        SimilarityFactorClass.LINEAGE,
                    ],
                ),
            ),
            InferredNeighborClass(
                urn=make_dataset_urn("postgres", "table1"),
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityAPI v1",
                    score=0.73,
                    similarityFactors=[
                        SimilarityFactorClass.DATASET_SCHEMA,
                        SimilarityFactorClass.ENTITY_NAME,
                    ],
                ),
            ),
        ]
    ),
)

idata = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn("snowflake", "table1"),
    aspect=InferredMetadataClass(
        inferredDescriptions=[
            InferredDescriptionClass(
                description="This is inferred description from dbt table table 1",
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={
                        "neighbourUrn": make_dataset_urn("dbt", "table1"),
                    },
                ),
            ),
            InferredDescriptionClass(
                description="This is inferred description from postgres table 1",
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("postgres", "table1")},
                ),
            ),
        ],
        inferredTags=[
            InferredTagsClass(
                tags=[make_tag_urn("tag_A"), make_tag_urn("tag_C")],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("dbt", "table1")},
                ),
            ),
            InferredTagsClass(
                tags=[make_tag_urn("tag_A"), make_tag_urn("tag_B")],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("postgres", "table1")},
                ),
            ),
        ],
        inferredGlossaryTerms=[
            InferredGlossaryTermsClass(
                glossaryTerms=[make_term_urn("term_email"), make_term_urn("term_pii")],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("dbt", "table1")},
                ),
            ),
            InferredGlossaryTermsClass(
                glossaryTerms=[make_term_urn("term_email")],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("postgres", "table1")},
                ),
            ),
        ],
        inferredOwners=[
            InferredOwnersClass(
                owners=[
                    OwnerClass(
                        owner=make_user_urn("user1"),
                        type=OwnershipTypeClass.DATA_STEWARD,
                    )
                ],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("dbt", "table1")},
                ),
            ),
            InferredOwnersClass(
                owners=[
                    OwnerClass(
                        owner=make_user_urn("user2"),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    )
                ],
                source=InferredMetadataSourceClass(
                    algorithm="DataHubSimilarityBasedPropagation v1",
                    context={"neighbourUrn": make_dataset_urn("postgres", "table1")},
                ),
            ),
        ],
    ),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# Emit mcps
rest_emitter.emit(ineighbors)
rest_emitter.emit(idata)
