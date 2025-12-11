# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from dataclasses import dataclass, field
from typing import Optional

from datahub.ingestion.api.source import SourceReport


@dataclass
class DataHubMockDataReport(SourceReport):
    first_urn_seen: Optional[str] = field(
        default=None,
        metadata={"description": "The first URN encountered during ingestion"},
    )
