import os

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

feature_urn = builder.make_ml_feature_urn(
    feature_table_name="user_features",
    feature_name="user_engagement_score",
)

source_dataset_urn = builder.make_dataset_urn(
    name="analytics.user_activity",
    platform="snowflake",
    env="PROD",
)

owner_urn = builder.make_user_urn("ml_platform_team")
tag_urn = builder.make_tag_urn("HighValue")
term_urn = builder.make_term_urn("EngagementMetrics")
domain_urn = builder.make_domain_urn("user_analytics")

feature_properties = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.MLFeaturePropertiesClass(
        description="Composite engagement score calculated from user activity metrics including "
        "page views, time on site, feature usage, and interaction frequency. "
        "Higher scores indicate more engaged users. Range: 0-100.",
        dataType="CONTINUOUS",
        version=models.VersionTagClass(versionTag="1.0"),
        sources=[source_dataset_urn],
        customProperties={
            "update_frequency": "daily",
            "calculation_method": "weighted_sum",
            "min_value": "0",
            "max_value": "100",
        },
    ),
)

ownership = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.OwnershipClass(
        owners=[
            models.OwnerClass(
                owner=owner_urn,
                type=models.OwnershipTypeClass.DATAOWNER,
            )
        ]
    ),
)

tags = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.GlobalTagsClass(tags=[models.TagAssociationClass(tag=tag_urn)]),
)

terms = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.GlossaryTermsClass(
        terms=[models.GlossaryTermAssociationClass(urn=term_urn)],
        auditStamp=models.AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
    ),
)

domains = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.DomainsClass(domains=[domain_urn]),
)

for mcp in [feature_properties, ownership, tags, terms, domains]:
    emitter.emit(mcp)
