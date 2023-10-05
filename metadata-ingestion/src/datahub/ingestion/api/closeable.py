from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Optional, Type, TypeVar

_Self = TypeVar("_Self", bound="Closeable")


class Closeable(AbstractContextManager):
    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self: _Self) -> _Self:
        # This method is mainly required for type checking.
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()
