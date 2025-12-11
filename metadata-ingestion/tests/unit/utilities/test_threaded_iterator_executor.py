# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor


def test_threaded_iterator_executor():
    def table_of(i):
        for j in range(1, 11):
            yield f"{i}x{j}={i * j}"

    assert {
        res
        for res in ThreadedIteratorExecutor.process(
            table_of, [(i,) for i in range(1, 30)], max_workers=2
        )
    } == {x for i in range(1, 30) for x in table_of(i)}
