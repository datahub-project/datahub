import os

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

source_dataset = builder.make_dataset_urn(
    name="analytics.users",
    platform="snowflake",
    env="PROD",
)

features_config = [
    {
        "name": "age",
        "description": "User age in years",
        "data_type": "CONTINUOUS",
    },
    {
        "name": "country",
        "description": "User country of residence",
        "data_type": "NOMINAL",
    },
    {
        "name": "is_verified",
        "description": "Whether user email is verified",
        "data_type": "BINARY",
    },
    {
        "name": "total_orders",
        "description": "Total number of orders placed",
        "data_type": "COUNT",
    },
    {
        "name": "signup_hour",
        "description": "Hour of day user signed up",
        "data_type": "TIME",
    },
]

mcps = []

for feature_config in features_config:
    feature_urn = builder.make_ml_feature_urn(
        feature_table_name="user_features",
        feature_name=feature_config["name"],
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=feature_urn,
        aspect=models.MLFeaturePropertiesClass(
            description=feature_config["description"],
            dataType=feature_config["data_type"],
            sources=[source_dataset],
        ),
    )
    mcps.append(mcp)

for mcp in mcps:
    emitter.emit(mcp)

print(f"Created {len(mcps)} features in feature namespace 'user_features'")
