from datahub.configuration.common import MetaError

# TODO: Move all error types to this file.


class SdkUsageError(MetaError):
    pass


class AlreadyExistsError(SdkUsageError):
    pass


class ItemNotFoundError(SdkUsageError):
    pass
