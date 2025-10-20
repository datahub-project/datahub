# Mypy stub for datahub.metadata.urns
# This extends the base module with acryl-cloud specific URNs

# Re-export everything from the base module
from datahub.metadata.urns import *  # type: ignore[misc]

# Import acryl-cloud specific URNs to make them available
from acryl_datahub_cloud.metadata._urns.urn_defs import *  # type: ignore[misc]