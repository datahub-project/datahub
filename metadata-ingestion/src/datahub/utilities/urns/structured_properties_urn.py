# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import StructuredPropertyUrn

__all__ = ["StructuredPropertyUrn", "make_structured_property_urn"]


def make_structured_property_urn(structured_property_id: str) -> str:
    return str(StructuredPropertyUrn.from_string(structured_property_id))
