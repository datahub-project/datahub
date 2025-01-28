from datahub.configuration.common import MetaError

# TODO: Move all other error types to this file.


class SdkUsageError(MetaError):
    pass


class AlreadyExistsError(SdkUsageError):
    pass


class ItemNotFoundError(SdkUsageError):
    pass


class SchemaFieldKeyError(SdkUsageError, KeyError):
    pass


class HiddenEditWarning(Warning):
    pass
