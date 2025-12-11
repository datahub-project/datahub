# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from datahub.utilities.topological_sort import topological_sort


def test_topological_sort_valid():
    nodes = ["a", "b", "c", "d", "e", "f"]
    edges = [
        ("a", "d"),
        ("f", "b"),
        ("b", "d"),
        ("f", "a"),
        ("d", "c"),
    ]

    # This isn't the only valid topological sort order.
    expected_order = ["e", "f", "b", "a", "d", "c"]
    assert list(topological_sort(nodes, edges)) == expected_order


def test_topological_sort_invalid():
    nodes = ["a", "b", "c", "d", "e", "f"]
    edges = [
        ("a", "d"),
        ("f", "b"),
        ("b", "d"),
        ("f", "a"),
        ("d", "c"),
        ("c", "f"),
    ]

    with pytest.raises(ValueError, match="cycle"):
        list(topological_sort(nodes, edges))
