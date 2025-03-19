from typing import Any, Union

import h5py
import numpy as np

from datahub.ingestion.source.hdf5.config import HDF5SourceConfig


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
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8")

    if isinstance(value, (str, np.str_, np.string_)):
        return str(value)

    if np.issubdtype(type(value), np.floating):
        if np.isnan(value):
            return "NaN"
        elif np.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"

    if isinstance(value, np.ndarray):
        if value.size == 1:  # Single-element array
            return numpy_value_to_string(value.item())
        else:
            return np.array2string(value, separator=", ")

    if np.issubdtype(type(value), np.datetime64):
        return str(value)

    return str(value)


def get_column_name(config: HDF5SourceConfig, index: int) -> str:
    return f"col{index}" if config.row_orientation is False else f"row{index}"


def get_column_count(config: HDF5SourceConfig, shape: tuple) -> int:
    if len(shape) == 1:
        return 1
    else:
        rows = shape[0]
        columns = shape[1]
        return columns if config.row_orientation is False else rows


def get_column_values(
    config: HDF5SourceConfig, dataset: h5py.Dataset, index: int
) -> list:
    if len(dataset.shape) == 1:
        return dataset[:].tolist()
    else:
        return (
            dataset[:, index].tolist()
            if config.row_orientation is False
            else dataset[index, :].tolist()
        )
