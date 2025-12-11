# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from unittest.mock import MagicMock

import pytest

from datahub.emitter.composite_emitter import CompositeEmitter
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile


@pytest.fixture
def mock_emitters():
    return [MagicMock(spec=Emitter), MagicMock(spec=Emitter)]


def test_composite_emitter_emit(mock_emitters):
    composite_emitter = CompositeEmitter(mock_emitters)
    item = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        aspect=DatasetProfile(
            rowCount=2000,
            columnCount=15,
            timestampMillis=1626995099686,
        ),
    )
    callback = MagicMock()

    composite_emitter.emit(item, callback)

    mock_emitters[0].emit.assert_called_once_with(item, callback)
    mock_emitters[1].emit.assert_called_once_with(item)
    assert mock_emitters[0].emit.call_count == 1
    assert mock_emitters[1].emit.call_count == 1


def test_composite_emitter_flush(mock_emitters):
    composite_emitter = CompositeEmitter(mock_emitters)

    composite_emitter.flush()

    for emitter in mock_emitters:
        emitter.flush.assert_called_once()
