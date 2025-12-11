# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

feature_urn = builder.make_ml_feature_urn(
    feature_table_name="user_features",
    feature_name="days_since_signup",
)

users_table_urn = builder.make_dataset_urn(
    name="analytics.users",
    platform="snowflake",
    env="PROD",
)

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=feature_urn,
    aspect=models.MLFeaturePropertiesClass(
        description="Number of days since the user created their account, "
        "calculated as the difference between current date and signup_date. "
        "Used for cohort analysis and lifecycle stage segmentation.",
        dataType="COUNT",
        sources=[users_table_urn],
    ),
)

emitter.emit(metadata_change_proposal)
