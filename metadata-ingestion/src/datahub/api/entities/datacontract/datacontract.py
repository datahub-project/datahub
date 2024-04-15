import collections
from typing import Iterable, List, Optional, Tuple

from ruamel.yaml import YAML
from typing_extensions import Literal

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datacontract.data_quality_assertion import (
    DataQualityAssertion,
)
from datahub.api.entities.datacontract.freshness_assertion import FreshnessAssertion
from datahub.api.entities.datacontract.schema_assertion import SchemaAssertion
from datahub.configuration.pydantic_migration_helpers import (
    v1_ConfigModel,
    v1_Field,
    v1_validator,
)
from datahub.emitter.mce_builder import datahub_guid, make_assertion_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataContractPropertiesClass,
    DataContractStateClass,
    DataContractStatusClass,
    DataQualityContractClass,
    FreshnessContractClass,
    SchemaContractClass,
    StatusClass,
)
from datahub.utilities.urns.urn import guess_entity_type


class DataContract(v1_ConfigModel):
    """A yml representation of a Data Contract.

    This model is used as a simpler, Python-native representation of a DataHub data contract.
    It can be easily parsed from a YAML file, and can be easily converted into series of MCPs
    that can be emitted to DataHub.
    """

    version: Literal[1]

    id: Optional[str] = v1_Field(
        default=None,
        alias="urn",
        description="The data contract urn. If not provided, one will be generated.",
    )
    entity: str = v1_Field(
        description="The entity urn that the Data Contract is associated with"
    )
    # TODO: add support for properties
    # properties: Optional[Dict[str, str]] = None

    schema_field: Optional[SchemaAssertion] = v1_Field(default=None, alias="schema")

    freshness: Optional[FreshnessAssertion] = v1_Field(default=None)

    # TODO: Add a validator to ensure that ids are unique
    data_quality: Optional[List[DataQualityAssertion]] = v1_Field(default=None)

    _original_yaml_dict: Optional[dict] = None

    @v1_validator("data_quality")  # type: ignore
    def validate_data_quality(
        cls, data_quality: Optional[List[DataQualityAssertion]]
    ) -> Optional[List[DataQualityAssertion]]:
        if data_quality:
            # Raise an error if there are duplicate ids.
            id_counts = collections.Counter(dq_check.id for dq_check in data_quality)
            duplicates = [id for id, count in id_counts.items() if count > 1]

            if duplicates:
                raise ValueError(
                    f"Got multiple data quality tests with the same type or ID: {duplicates}. Set a unique ID for each data quality test."
                )

        return data_quality

    @property
    def urn(self) -> str:
        if self.id:
            assert guess_entity_type(self.id) == "dataContract"
            return self.id

        # Data contract urns are stable
        guid_obj = {"entity": self.entity}
        urn = f"urn:li:dataContract:{datahub_guid(guid_obj)}"
        return urn

    def _generate_freshness_assertion(
        self, freshness: FreshnessAssertion
    ) -> Tuple[str, List[MetadataChangeProposalWrapper]]:
        guid_dict = {
            "contract": self.urn,
            "entity": self.entity,
            "freshness": freshness.id,
        }
        assertion_urn = builder.make_assertion_urn(builder.datahub_guid(guid_dict))

        return (
            assertion_urn,
            freshness.generate_mcp(assertion_urn, self.entity),
        )

    def _generate_schema_assertion(
        self, schema_metadata: SchemaAssertion
    ) -> Tuple[str, List[MetadataChangeProposalWrapper]]:
        # ingredients for guid -> the contract id, the fact that this is a schema assertion and the entity on which the assertion is made
        guid_dict = {
            "contract": self.urn,
            "entity": self.entity,
            "schema": schema_metadata.id,
        }
        assertion_urn = make_assertion_urn(datahub_guid(guid_dict))

        return (
            assertion_urn,
            schema_metadata.generate_mcp(assertion_urn, self.entity),
        )

    def _generate_data_quality_assertion(
        self, data_quality: DataQualityAssertion
    ) -> Tuple[str, List[MetadataChangeProposalWrapper]]:
        guid_dict = {
            "contract": self.urn,
            "entity": self.entity,
            "data_quality": data_quality.id,
        }
        assertion_urn = make_assertion_urn(datahub_guid(guid_dict))

        return (
            assertion_urn,
            data_quality.generate_mcp(assertion_urn, self.entity),
        )

    def _generate_dq_assertions(
        self, data_quality_spec: List[DataQualityAssertion]
    ) -> Tuple[List[str], List[MetadataChangeProposalWrapper]]:
        assertion_urns = []
        assertion_mcps = []

        for dq_check in data_quality_spec:
            assertion_urn, assertion_mcp = self._generate_data_quality_assertion(
                dq_check
            )

            assertion_urns.append(assertion_urn)
            assertion_mcps.extend(assertion_mcp)

        return (assertion_urns, assertion_mcps)

    def generate_mcp(
        self,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        schema_assertion_urn = None
        if self.schema_field is not None:
            (
                schema_assertion_urn,
                schema_assertion_mcps,
            ) = self._generate_schema_assertion(self.schema_field)
            yield from schema_assertion_mcps

        freshness_assertion_urn = None
        if self.freshness:
            (
                freshness_assertion_urn,
                sla_assertion_mcps,
            ) = self._generate_freshness_assertion(self.freshness)
            yield from sla_assertion_mcps

        dq_assertions, dq_assertion_mcps = self._generate_dq_assertions(
            self.data_quality or []
        )
        yield from dq_assertion_mcps

        # Now that we've generated the assertions, we can generate
        # the actual data contract.
        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=self.urn,
            aspects=[
                DataContractPropertiesClass(
                    entity=self.entity,
                    schema=[SchemaContractClass(assertion=schema_assertion_urn)]
                    if schema_assertion_urn
                    else None,
                    freshness=[
                        FreshnessContractClass(assertion=freshness_assertion_urn)
                    ]
                    if freshness_assertion_urn
                    else None,
                    dataQuality=[
                        DataQualityContractClass(assertion=dq_assertion_urn)
                        for dq_assertion_urn in dq_assertions
                    ],
                ),
                # Also emit status.
                StatusClass(removed=False),
                # Emit the contract state as PENDING.
                DataContractStatusClass(state=DataContractStateClass.PENDING)
                if True
                else None,
            ],
        )

    @classmethod
    def from_yaml(
        cls,
        file: str,
    ) -> "DataContract":
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            orig_dictionary = yaml.load(fp)
            parsed_data_contract = DataContract.parse_obj(orig_dictionary)
            parsed_data_contract._original_yaml_dict = orig_dictionary
            return parsed_data_contract
