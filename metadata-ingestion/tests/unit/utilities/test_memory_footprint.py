# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from collections import defaultdict

from datahub.utilities import memory_footprint


def test_total_size_with_empty_dict():
    size = memory_footprint.total_size({})
    # Only asserting if it is bigger than 0 because the actual sizes differs per python version
    assert size > 0


def test_total_size_with_list():
    size = memory_footprint.total_size({"1": [1, 2, 3, 4]})
    # Only asserting if it is bigger than 0 because the actual sizes differs per python version
    assert size > 0


def test_total_size_with_none():
    size = memory_footprint.total_size(None)
    # Only asserting if it is bigger than 0 because the actual sizes differs per python version
    assert size > 0


def test_total_size_with_defaultdict():
    size = memory_footprint.total_size(defaultdict)
    # Only asserting if it is bigger than 0 because the actual sizes differs per python version
    assert size > 0
