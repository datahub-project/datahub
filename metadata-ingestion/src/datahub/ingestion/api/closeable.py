# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Optional, Type

from typing_extensions import Self


class Closeable(AbstractContextManager):
    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self) -> Self:
        # This method is mainly required for type checking.
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()
