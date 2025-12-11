# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List, Set

from datahub.emitter.mce_builder import (
    dataset_key_to_urn,
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.metadata.schema_classes import DatasetKeyClass


class CheckpointStateUtil:
    """
    A utility class to provide common functionalities around Urn and CheckPointState of different DataHub entities
    """

    @staticmethod
    def get_separator() -> str:
        # Unique small string not allowed in URNs.
        return "||"

    @staticmethod
    def get_encoded_urns_not_in(
        encoded_urns_1: List[str], encoded_urns_2: List[str]
    ) -> Set[str]:
        return set(encoded_urns_1) - set(encoded_urns_2)

    @staticmethod
    def get_urn_from_encoded_dataset(encoded_urn: str) -> str:
        platform, name, env = encoded_urn.split(CheckpointStateUtil.get_separator())
        return dataset_key_to_urn(
            DatasetKeyClass(
                platform=make_data_platform_urn(platform), name=name, origin=env
            )
        )

    @staticmethod
    def get_urn_from_encoded_topic(encoded_urn: str) -> str:
        platform, name, env = encoded_urn.split(CheckpointStateUtil.get_separator())
        return make_dataset_urn(platform, name, env)
