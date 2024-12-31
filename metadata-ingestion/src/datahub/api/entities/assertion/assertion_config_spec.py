from typing import List, Optional

from ruamel.yaml import YAML
from typing_extensions import Literal

from datahub.api.entities.assertion.datahub_assertion import DataHubAssertion
from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field


class AssertionsConfigSpec(v1_ConfigModel):
    """
    Declarative configuration specification for datahub assertions.

    This model is used as a simpler, Python-native representation to define assertions.
    It can be easily parsed from a equivalent YAML file.

    Currently, this is converted into series of assertion MCPs that can be emitted to DataHub.
    In future, this would invoke datahub GraphQL API to upsert assertions.
    """

    version: Literal[1]

    id: Optional[str] = v1_Field(
        default=None,
        alias="namespace",
        description="Unique identifier of assertions configuration file",
    )

    assertions: List[DataHubAssertion]

    @classmethod
    def from_yaml(
        cls,
        file: str,
    ) -> "AssertionsConfigSpec":
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            orig_dictionary = yaml.load(fp)
            parsed_spec = AssertionsConfigSpec.parse_obj(orig_dictionary)
            # parsed_spec._original_yaml_dict = orig_dictionary
            return parsed_spec
