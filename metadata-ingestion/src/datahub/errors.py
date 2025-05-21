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
