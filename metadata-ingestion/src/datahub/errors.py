# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.configuration.common import MetaError

# TODO: Move all other error types to this file.


class SdkUsageError(MetaError):
    pass


class AlreadyExistsError(SdkUsageError):
    pass


class ItemNotFoundError(SdkUsageError):
    pass


class MultipleItemsFoundError(SdkUsageError):
    pass


class SchemaFieldKeyError(SdkUsageError, KeyError):
    pass


class IngestionAttributionWarning(Warning):
    pass


class MultipleSubtypesWarning(Warning):
    pass


class SearchFilterWarning(Warning):
    pass


class ExperimentalWarning(Warning):
    pass


class APITracingWarning(Warning):
    pass


class DataHubDeprecationWarning(DeprecationWarning):
    pass
