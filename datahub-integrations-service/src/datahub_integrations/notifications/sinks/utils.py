import time
from typing import Any, Callable


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 3,
    backoff_factor: int = 2,
    initial_backoff: int = 1,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Retries a function with exponential backoff.

    Parameters:
        func (Callable): The function to execute.
        max_attempts (int): Maximum number of retries.
        backoff_factor (int): Factor by which to multiply the backoff time for each retry.
        initial_backoff (int): Initial backoff time in seconds.
        *args: Variable length argument list for the function.
        **kwargs: Arbitrary keyword arguments for the function.

    Returns:
        Any: The result of the function call, if successful.

    Raises:
        Exception: The last exception caught if the function fails after all retries.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            return func(
                *args, **kwargs
            )  # Attempt to call the function with the provided arguments
        except Exception:
            if attempt == max_attempts - 1:
                raise  # Re-raise the last exception after all retries have failed
            else:
                backoff_time = initial_backoff * (backoff_factor**attempt)
                time.sleep(
                    backoff_time
                )  # Wait for an exponentially increasing backoff period
                attempt += 1
