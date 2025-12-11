# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.utilities.prefix_batch_builder import PrefixGroup, build_prefix_batches


def test_build_prefix_batches_empty_input():
    assert build_prefix_batches([], 10, 5) == [[PrefixGroup(prefix="", names=[])]]


def test_build_prefix_batches_single_group():
    names = ["apple", "applet", "application"]
    expected = [[PrefixGroup(prefix="", names=names)]]
    assert build_prefix_batches(names, 10, 5) == expected


def test_build_prefix_batches_multiple_groups():
    names = ["apple", "applet", "banana", "band", "bandana"]
    expected = [
        [PrefixGroup(prefix="a", names=["apple", "applet"])],
        [PrefixGroup(prefix="b", names=["banana", "band", "bandana"])],
    ]
    assert build_prefix_batches(names, 4, 5) == expected


def test_build_prefix_batches_exceeds_max_batch_size():
    names = [
        "app",
        "apple",
        "applet",
        "application",
        "banana",
        "band",
        "bandana",
        "candy",
        "candle",
        "dog",
    ]
    expected = [
        [PrefixGroup(prefix="app", names=["app"], exact_match=True)],
        [PrefixGroup(prefix="appl", names=["apple", "applet", "application"])],
        [PrefixGroup(prefix="b", names=["banana", "band", "bandana"])],
        [
            PrefixGroup(prefix="c", names=["candle", "candy"]),
            PrefixGroup(prefix="d", names=["dog"]),
        ],
    ]
    assert build_prefix_batches(names, 3, 2) == expected
