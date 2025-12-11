# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
    feature_name="total_spend",
)

dataset_urn = builder.make_dataset_urn(
    name="analytics.orders",
    platform="snowflake",
    env="PROD",
)

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.MLFeaturePropertiesClass(
        description="Total amount spent by user across all orders. "
        "Version 2.0 now includes refunds and returns, providing net spend instead of gross. "
        "Changed from gross spend calculation in v1.0.",
        dataType="CONTINUOUS",
        version=models.VersionTagClass(versionTag="2.0"),
        sources=[dataset_urn],
    ),
)

emitter.emit(metadata_change_proposal)
