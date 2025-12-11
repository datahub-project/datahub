# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from abc import ABC, abstractmethod
from typing import Any, Dict

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.sink import Sink


class PipelineRunListener(ABC):
    @abstractmethod
    def on_start(self, ctx: PipelineContext) -> None:
        # Perform
        pass

    @abstractmethod
    def on_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        pass

    @classmethod
    @abstractmethod
    def create(
        cls,
        config_dict: Dict[str, Any],
        ctx: PipelineContext,
        sink: Sink,
    ) -> "PipelineRunListener":
        # Creation and initialization.
        pass
