# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

"""
Secret masking system for DataHub ingestion.
"""

__all__ = [
    "SecretMaskingFilter",
    "StreamMaskingWrapper",
    "install_masking_filter",
    "uninstall_masking_filter",
    "SecretRegistry",
    "is_masking_enabled",
    "initialize_secret_masking",
    "get_masking_safe_logger",
]

from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    StreamMaskingWrapper,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled
