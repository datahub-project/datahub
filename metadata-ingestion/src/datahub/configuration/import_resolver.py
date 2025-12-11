# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import TYPE_CHECKING, Type, TypeVar, Union

from pydantic import field_validator

from datahub.ingestion.api.registry import import_path

if TYPE_CHECKING:
    from pydantic.deprecated.class_validators import V1Validator

_T = TypeVar("_T")


def _pydantic_resolver(cls: Type, v: Union[str, _T]) -> _T:
    return import_path(v) if isinstance(v, str) else v


def pydantic_resolve_key(field: str) -> "V1Validator":
    return field_validator(field, mode="before")(_pydantic_resolver)
