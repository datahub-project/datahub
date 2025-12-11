# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)

upstream_table_1 = UpstreamClass(
    dataset=builder.make_dataset_urn("bigquery", "upstream_table_1", "PROD"),
    type=DatasetLineageTypeClass.TRANSFORMED,
)
upstream_tables: List[UpstreamClass] = [upstream_table_1]
upstream_table_2 = UpstreamClass(
    dataset=builder.make_dataset_urn("bigquery", "upstream_table_2", "PROD"),
    type=DatasetLineageTypeClass.TRANSFORMED,
)
upstream_tables.append(upstream_table_2)

# Construct a lineage object.
upstream_lineage = UpstreamLineage(upstreams=upstream_tables)

# Construct a MetadataChangeProposalWrapper object.
lineage_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("bigquery", "downstream"),
    aspect=upstream_lineage,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mcp(lineage_mcp)
