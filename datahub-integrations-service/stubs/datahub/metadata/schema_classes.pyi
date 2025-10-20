# Mypy stub for datahub.metadata.schema_classes
# This extends the base module with acryl-cloud specific classes

# Re-export everything from the base module
from datahub.metadata.schema_classes import *  # type: ignore[misc]

# Import acryl-cloud specific classes to make them available
from acryl_datahub_cloud.metadata.schema_classes import *  # type: ignore[misc]

# Note: _Aspect exists at runtime but mypy can't see it via star imports