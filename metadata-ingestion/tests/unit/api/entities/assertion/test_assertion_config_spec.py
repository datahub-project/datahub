# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.api.entities.assertion.assertion_config_spec import AssertionsConfigSpec


def test_assertion_config_spec_parses_correct_type(pytestconfig):
    config_file = (
        pytestconfig.rootpath
        / "tests/unit/api/entities/assertion/test_assertion_config.yml"
    )

    config_spec = AssertionsConfigSpec.from_yaml(config_file)
    assert config_spec.version == 1
    assert config_spec.id == "test-config-id-1"
    assert len(config_spec.assertions) == 5
