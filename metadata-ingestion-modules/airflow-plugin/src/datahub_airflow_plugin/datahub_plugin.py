# This package serves as a shim, but the actual implementation lives in datahub_provider
# from the acryl-datahub package. We leave this shim here to avoid breaking existing
# Airflow installs.
from datahub_provider._plugin import DatahubPlugin  # noqa: F401
