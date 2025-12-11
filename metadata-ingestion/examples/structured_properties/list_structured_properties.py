# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# Usage: python3 list_structured_properties.py
# Expected Output: List of structured properties
# This script lists all structured properties in DataHub
from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.ingestion.graph.client import get_default_graph

with get_default_graph() as graph:
    structuredproperties = StructuredProperties.list(graph)
    for structuredproperty in structuredproperties:
        print(structuredproperty.dict())
