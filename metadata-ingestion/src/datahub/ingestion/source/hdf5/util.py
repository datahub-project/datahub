from typing import Any, Union

import numpy as np


def decode_type(f_type: Union[str, tuple, np.dtype]) -> str:
    if isinstance(f_type, np.dtype):
        return f_type.name
    elif type(f_type) is str:
        return np.dtype(f_type).name
    elif type(f_type) is tuple:
        if type(f_type[-1]) is dict:
            encoding = f_type[-1].get("h5py_encoding")
            if encoding == "ascii" or encoding == "utf-8":
                return "string_"
        else:
            return np.dtype(f_type[0]).name
    return "unknown"


def numpy_value_to_string(value: Any) -> str:
    # Handle bytes
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")

    # If it's already a string or string-like, just convert to Python string
    if isinstance(value, (str, np.str_, np.string_)):
        return str(value)

    # Handle NaN and Infinity specially
    if np.issubdtype(type(value), np.floating):
        if np.isnan(value):
            return "NaN"
        elif np.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"

    # Handle arrays
    if isinstance(value, np.ndarray):
        if value.size == 1:  # Single-element array
            return numpy_value_to_string(value.item())
        else:
            return np.array2string(value, separator=", ")

    # Handle datetime types
    if np.issubdtype(type(value), np.datetime64):
        return str(value)

    # Handle other scalar types
    return str(value)
