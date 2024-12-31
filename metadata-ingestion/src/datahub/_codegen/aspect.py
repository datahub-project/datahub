from typing import ClassVar

from avrogen.dict_wrapper import DictWrapper


class _Aspect(DictWrapper):
    """Base class for all aspects types.

    All codegened types inherit from DictWrapper, either directly or indirectly.
    Types that are aspects inherit directly from _Aspect.
    """

    ASPECT_NAME: ClassVar[str] = None  # type: ignore
    ASPECT_TYPE: ClassVar[str] = "default"
    ASPECT_INFO: ClassVar[dict] = None  # type: ignore

    def __init__(self):
        if type(self) is _Aspect:
            # Ensure that it cannot be instantiated directly, as
            # per https://stackoverflow.com/a/7989101/5004662.
            raise TypeError(
                "_Aspect is an abstract class, and cannot be instantiated directly."
            )
        super().__init__()

    @classmethod
    def get_aspect_name(cls) -> str:
        return cls.ASPECT_NAME  # type: ignore

    @classmethod
    def get_aspect_type(cls) -> str:
        return cls.ASPECT_TYPE

    @classmethod
    def get_aspect_info(cls) -> dict:
        return cls.ASPECT_INFO
