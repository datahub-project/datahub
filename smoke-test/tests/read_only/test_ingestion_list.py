# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utilities.metadata_operations import list_ingestion_sources


@pytest.mark.read_only
def test_policies_are_accessible(auth_session):
    res_data = list_ingestion_sources(auth_session)
    assert res_data, f"Received listIngestionSources were {res_data}"
    add_datahub_stats("num-ingestion-sources", res_data["total"])

    if res_data["total"] > 0:
        for ingestion_source in res_data.get("ingestionSources"):
            name = ingestion_source.get("name")
            source_type = ingestion_source.get("type")
            urn = ingestion_source.get("urn")
            version = ingestion_source.get("config", {}).get("version")
            add_datahub_stats(
                f"ingestion-source-{urn}",
                {"name": name, "version": version, "source_type": source_type},
            )
