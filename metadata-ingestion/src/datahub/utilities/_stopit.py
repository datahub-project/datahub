# Vendored and trimmed from stopit 1.1.2 (https://github.com/glenfant/stopit).
# stopit is unmaintained (last release 2018) and imports pkg_resources at load,
# so it cannot be installed alongside setuptools>=82. Only the threading-based
# timeout is kept; the mechanism is unchanged from upstream (ctypes
# PyThreadState_SetAsyncExc + threading.Timer), so behaviour is identical.
#
# Copyright (c) 2018 Gilles Lenfant
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import ctypes
import logging
import threading
from types import TracebackType
from typing import Optional, Type

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    """Raised when the block under context management takes longer to complete
    than the allowed maximum timeout value."""


def async_raise(target_tid: int, exception: Type[BaseException]) -> None:
    """Raise an asynchronous exception in another thread.

    See https://docs.python.org/3/c-api/init.html#c.PyThreadState_SetAsyncExc.
    """
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(target_tid), ctypes.py_object(exception)
    )
    if ret == 0:
        raise ValueError(f"Invalid thread ID {target_tid}")
    elif ret > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class BaseTimeout:
    """Context manager that limits the execution time of a block."""

    EXECUTED, EXECUTING, TIMED_OUT, INTERRUPTED = range(4)

    def __init__(self, seconds: float, swallow_exc: bool = True) -> None:
        self.seconds = seconds
        self.swallow_exc = swallow_exc
        self.state = BaseTimeout.EXECUTED

    def __enter__(self) -> "BaseTimeout":
        self.state = BaseTimeout.EXECUTING
        self.setup_interrupt()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if exc_type is TimeoutException:
            if self.state != BaseTimeout.TIMED_OUT:
                self.state = BaseTimeout.INTERRUPTED
                self.suppress_interrupt()
            logger.warning(
                f"Code block execution exceeded {self.seconds} seconds timeout",
                exc_info=exc_val,
            )
            return self.swallow_exc
        else:
            if exc_type is None:
                self.state = BaseTimeout.EXECUTED
            self.suppress_interrupt()
        return False

    def suppress_interrupt(self) -> None:
        """Remove/neutralize the feature that interrupts the executed block."""
        raise NotImplementedError

    def setup_interrupt(self) -> None:
        """Install/initialize the feature that interrupts the executed block."""
        raise NotImplementedError


class ThreadingTimeout(BaseTimeout):
    """Context manager limiting the execution time of a block by launching an
    asynchronous exception into the calling thread."""

    def __init__(self, seconds: float, swallow_exc: bool = True) -> None:
        super().__init__(seconds, swallow_exc)
        ident = threading.current_thread().ident
        assert ident is not None  # a started thread always has an ident
        self.target_tid: int = ident
        self.timer: Optional[threading.Timer] = None

    def stop(self) -> None:
        """Called by the timer thread at timeout. Raises a TimeoutException in
        the caller thread."""
        self.state = BaseTimeout.TIMED_OUT
        async_raise(self.target_tid, TimeoutException)

    def setup_interrupt(self) -> None:
        self.timer = threading.Timer(self.seconds, self.stop)
        self.timer.start()

    def suppress_interrupt(self) -> None:
        assert self.timer is not None
        self.timer.cancel()
