# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.source.redshift.config import RedshiftConfig


def test_incremental_lineage_default_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.incremental_lineage is False
