from .build_info import BuildInfo
from .utils import create_instance_id, get_host_address, get_hostname

DATAHUB_EXECUTOR_IDENTITY_BUILD_INFO = BuildInfo()
DATAHUB_EXECUTOR_IDENTITY_HOSTNAME = get_hostname()
DATAHUB_EXECUTOR_IDENTITY_ADDRESS = get_host_address(DATAHUB_EXECUTOR_IDENTITY_HOSTNAME)
DATAHUB_EXECUTOR_IDENTITY = create_instance_id(DATAHUB_EXECUTOR_IDENTITY_ADDRESS)
