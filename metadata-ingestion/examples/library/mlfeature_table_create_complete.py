import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

# Step 1: Create the source dataset for lineage
dataset_urn = builder.make_dataset_urn(
    name="customer_transactions", platform="snowflake", env="PROD"
)

# Step 2: Create the primary key entity
primary_key_urn = builder.make_ml_primary_key_urn(
    feature_table_name="transaction_features",
    primary_key_name="transaction_id",
)

primary_key_properties = models.MLPrimaryKeyPropertiesClass(
    description="Unique identifier for each transaction",
    dataType="TEXT",
    sources=[dataset_urn],
)

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=primary_key_urn,
        aspect=primary_key_properties,
    )
)

# Step 3: Create the feature entities
feature_1_urn = builder.make_ml_feature_urn(
    feature_name="transaction_amount",
    feature_table_name="transaction_features",
)

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=feature_1_urn,
        aspect=models.MLFeaturePropertiesClass(
            description="Total amount of the transaction in USD",
            dataType="CONTINUOUS",
            sources=[dataset_urn],
        ),
    )
)

feature_2_urn = builder.make_ml_feature_urn(
    feature_name="is_fraud",
    feature_table_name="transaction_features",
)

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=feature_2_urn,
        aspect=models.MLFeaturePropertiesClass(
            description="Binary indicator of fraudulent transaction",
            dataType="BINARY",
            sources=[dataset_urn],
        ),
    )
)

# Step 4: Create the feature table with all properties
feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="transaction_features", platform="feast"
)

feature_table_properties = models.MLFeatureTablePropertiesClass(
    description="Real-time transaction features for fraud detection models",
    mlFeatures=[feature_1_urn, feature_2_urn],
    mlPrimaryKeys=[primary_key_urn],
    customProperties={
        "update_frequency": "real-time",
        "team": "fraud-detection",
        "critical": "true",
    },
)

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=feature_table_urn,
        aspect=feature_table_properties,
    )
)

# Step 5: Add tags for categorization
emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=feature_table_urn,
        aspect=models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(tag=builder.make_tag_urn("Fraud Detection")),
                models.TagAssociationClass(
                    tag=builder.make_tag_urn("Real-time Features")
                ),
            ]
        ),
    )
)

# Step 6: Add ownership
emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=feature_table_urn,
        aspect=models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner=builder.make_user_urn("data_science_team"),
                    type=models.OwnershipTypeClass.DATAOWNER,
                )
            ]
        ),
    )
)

print(f"Successfully created feature table: {feature_table_urn}")
