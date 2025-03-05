import functools
from typing import Any, Callable, Optional, TypeVar, cast

# Define a type variable for the decorator
F = TypeVar("F", bound=Callable[..., Any])


# Check which Pydantic version is installed
def get_pydantic_version() -> int:
    """Determine if Pydantic v1 or v2 is installed."""
    try:
        import pydantic

        version = pydantic.__version__
        return 1 if version.startswith("1.") else 2
    except (ImportError, AttributeError):
        # Default to v1 if we can't determine version
        return 1


PYDANTIC_VERSION = get_pydantic_version()


# Create compatibility layer for dict-like methods
def compat_dict_method(v1_method: Optional[Callable] = None) -> Callable:
    """
    Decorator to make a dict method work with both Pydantic v1 and v2.

    In v1: Uses the decorated method (typically dict)
    In v2: Redirects to model_dump with appropriate parameter mapping
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if PYDANTIC_VERSION >= 2:
                # Map v1 parameters to v2 parameters
                # exclude -> exclude
                # exclude_unset -> exclude_unset
                # exclude_defaults -> exclude_defaults
                # exclude_none -> exclude_none
                # by_alias -> by_alias
                model_dump_kwargs = kwargs.copy()

                # Handle the 'exclude' parameter differently between versions
                exclude = kwargs.get("exclude", set())
                if isinstance(exclude, (set, dict)):
                    model_dump_kwargs["exclude"] = exclude

                return self.model_dump(**model_dump_kwargs)
            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    # Allow use as both @compat_dict_method and @compat_dict_method()
    if v1_method is None:
        return decorator
    return decorator(v1_method)
