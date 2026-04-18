import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="customer_features", platform="feast"
)

feature_table_properties = models.MLFeatureTablePropertiesClass(
    description="Customer demographic and behavioral features for churn prediction models. "
    "Updated daily from the customer data warehouse.",
    customProperties={
        "update_frequency": "daily",
        "feature_count": "25",
        "team": "customer-analytics",
        "sla_hours": "24",
    },
)

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=feature_table_urn,
    aspect=feature_table_properties,
)

emitter.emit(metadata_change_proposal)
