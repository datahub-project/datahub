# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


def datasetUrn(tbl):
    return builder.make_dataset_urn("hive", tbl)


def fldUrn(tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(tbl), fld)


fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[
            fldUrn("fct_users_deleted", "browser_id"),
            fldUrn("fct_users_created", "user_id"),
        ],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("logging_events", "browser")],
    ),
]


# this is just to check if any conflicts with existing Upstream, particularly the DownstreamOf relationship
upstream = Upstream(
    dataset=datasetUrn("fct_users_deleted"), type=DatasetLineageType.TRANSFORMED
)

fieldLineages = UpstreamLineage(
    upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
)

lineageMcp = MetadataChangeProposalWrapper(
    entityUrn=datasetUrn("logging_events"),
    aspect=fieldLineages,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(lineageMcp)
