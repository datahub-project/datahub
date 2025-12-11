# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/assertion_freshness.py
import os

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
    FreshnessCronScheduleClass,
)

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

dataset_urn = builder.make_dataset_urn(
    platform="redshift", name="prod.analytics.daily_metrics"
)

freshness_assertion_info = FreshnessAssertionInfoClass(
    type=FreshnessAssertionTypeClass.DATASET_CHANGE,
    entity=dataset_urn,
    schedule=FreshnessAssertionScheduleClass(
        type=FreshnessAssertionScheduleTypeClass.CRON,
        cron=FreshnessCronScheduleClass(
            cron="0 9 * * *",
            timezone="America/Los_Angeles",
            windowStartOffsetMs=None,
        ),
    ),
)

assertion_info = AssertionInfoClass(
    type=AssertionTypeClass.FRESHNESS,
    freshnessAssertion=freshness_assertion_info,
    description="Daily metrics table must be updated every day by 9 AM Pacific Time",
)

assertion_urn = builder.make_assertion_urn(
    builder.datahub_guid({"entity": dataset_urn, "type": "freshness-daily-9am"})
)

assertion_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_info,
)

emitter.emit_mcp(assertion_info_mcp)
print(f"Created freshness assertion: {assertion_urn}")
