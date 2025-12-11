# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pathlib

from datahub.utilities.yaml_sync_utils import YamlFileUpdater


def test_update_yaml_file(tmp_path: pathlib.Path) -> None:
    infile = tmp_path / "test.yml"

    # Note - this will drop the leading newline before the comment.
    infile.write_text(
        """
# this is a comment
#
obj:
  key1: value1

list_ty:
  - foo
  - key1: value1
    key2: value2
"""
    )
    # ind=4, bsi=2

    with YamlFileUpdater(infile) as doc:
        doc["foo"] = "bar"
        doc["list_ty"].append("baz")
        doc["list_ty"][1]["key1.5"] = "val1.5"

    assert (
        infile.read_text()
        == """# this is a comment
#
obj:
  key1: value1

list_ty:
  - foo
  - key1: value1
    key2: value2
    key1.5: val1.5
  - baz
foo: bar
"""
    )


def test_indentation_inference(tmp_path: pathlib.Path) -> None:
    infile = tmp_path / "test.yml"

    infile.write_text(
        """
# this is a comment
#
obj:
  key1: value1

list_ty:
- foo
- key1: value1
  key2: value2
"""
    )
    # ind=2, bsi=0

    with YamlFileUpdater(infile) as doc:
        doc["foo"] = "bar"

    assert (
        infile.read_text()
        == """# this is a comment
#
obj:
  key1: value1

list_ty:
- foo
- key1: value1
  key2: value2
foo: bar
"""
    )


# TODO: This yaml indentation will fail, because the mapping indent is 2 but the sequence indent is 4.
"""
x:
  y:
    - b: 1
    - 2
"""
