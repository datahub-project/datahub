# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.utilities.parsing_util import (
    get_first_missing_key,
    get_first_missing_key_any,
)


def test_get_missing_key():
    assert get_first_missing_key({}, [""]) == ""
    assert get_first_missing_key({"a": 1}, ["a"]) is None
    assert get_first_missing_key({"a": {"b": 1}}, ["a", "b"]) is None
    assert get_first_missing_key({"a": {"b": 1}}, ["a", "c"]) == "c"
    assert get_first_missing_key({"a": ["b", "c", "d"]}, ["a", "c"]) == "c"


def test_get_missing_key_any():
    assert get_first_missing_key_any({}, ["a"]) == "a"
    assert get_first_missing_key_any({"a": 1, "b": 2}, ["a", "b"]) is None
