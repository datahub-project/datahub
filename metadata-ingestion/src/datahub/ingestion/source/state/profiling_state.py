# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Dict

import pydantic

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class ProfilingCheckpointState(CheckpointStateBase):
    """
    Base class for representing the checkpoint state for all profiling based sources.
    Stores the last successful profiling time per urn.
    Subclasses can define additional state as appropriate.
    """

    # Last profiled stores urn, last_profiled timestamp millis in a dict
    last_profiled: Dict[str, pydantic.PositiveInt]
