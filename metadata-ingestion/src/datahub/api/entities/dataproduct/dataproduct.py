from __future__ import annotations

import time
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pydantic
from ruamel.yaml import YAML

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    KafkaAuditHeaderClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SystemMetadataClass,
    TagAssociationClass,
)
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.urns.urn import Urn

if TYPE_CHECKING:
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter


class DataProductGenerationConfig(ConfigModel):
    validate_assets: bool = True


class DataProduct(ConfigModel):
    """This is a DataProduct class which represents a DataProduct

    Args:
        id (str): The id of the Data Product
        domain (str): The domain that the Data Product belongs to. Either as a name or a fully-qualified urn.
        owners (Optional[List[str, Ownership]]): A list of owners and their types.
        display_name (Optional[str]): The name of the Data Product to display in the UI
        description (Optional[str]): A documentation string for the Data Product
        tags (Optional[List[str]]): An array of tags (either bare ids or urns) for the Data Product
        terms (Optional[List[str]]): An array of terms (either bare ids or urns) for the Data Product
        assets (List[str]): An array of entity urns that are part of the Data Product
    """

    class Ownership(ConfigModel):
        id: str
        type: str

        @pydantic.validator("type")
        def ownership_type_must_be_mappable(cls, v: str) -> str:
            _ownership_types = [
                OwnershipTypeClass.BUSINESS_OWNER,
                OwnershipTypeClass.CONSUMER,
                OwnershipTypeClass.DATA_STEWARD,
                OwnershipTypeClass.DATAOWNER,
                OwnershipTypeClass.DELEGATE,
                OwnershipTypeClass.DEVELOPER,
                OwnershipTypeClass.NONE,
                OwnershipTypeClass.PRODUCER,
                OwnershipTypeClass.STAKEHOLDER,
                OwnershipTypeClass.TECHNICAL_OWNER,
            ]
            if v.upper() not in _ownership_types:
                raise ValueError(f"Ownership type {v} not in {_ownership_types}")

            return v.upper()

    id: str
    domain: str
    _resolved_domain_urn: str
    assets: List[str]
    display_name: Optional[str] = None
    owners: Optional[List[Union[str, DataProduct.Ownership]]] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    terms: Optional[List[str]] = None

    @pydantic.validator("assets", each_item=True)
    def assets_must_be_urns(cls, v: str) -> str:
        try:
            Urn.create_from_string(v)
        except Exception as e:
            raise ValueError(f"asset {v} is not an urn", e)

        return v

    @property
    def urn(self) -> str:
        if self.id.startswith("urn:li:dataProduct:"):
            return self.id
        else:
            return f"urn:li:dataProduct:{self.id}"

    # If domain is an urn, we cache it in the private _resolved_domain_urn field
    # Otherwise, we expect the caller to populate this by using an external DomainRegistry
    @pydantic.validator("domain")
    def cache_resolved_domain_urn_if_urn(cls, v: str) -> str:
        if isinstance(v, str) and v.startswith("urn:li:domain:"):
            cls._resolved_domain_urn = v
        return v

    def _mint_auditstamp(self, message: str) -> AuditStampClass:
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _mint_owner(self, owner: Union[str, "DataProduct.Ownership"]) -> OwnerClass:
        if isinstance(owner, str):
            return OwnerClass(
                owner=builder.make_user_urn(owner),
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        else:
            assert isinstance(owner, DataProduct.Ownership)
            return OwnerClass(
                owner=builder.make_user_urn(owner.id),
                type=owner.type,
            )

    def generate_mcp(
        self,
        generation_config: DataProductGenerationConfig = DataProductGenerationConfig(),
    ) -> Iterable[MetadataChangeProposalWrapper]:

        if generation_config.validate_assets:
            # pass
            print(generation_config)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DataProductPropertiesClass(
                name=self.display_name,
                description=self.description,
                assets=[
                    DataProductAssociationClass(
                        destinationUrn=asset,
                        created=self._mint_auditstamp("yaml"),
                    )
                    for asset in self.assets
                ],
            ),
        )
        yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=DomainsClass(
                domains=[builder.make_domain_urn(self._resolved_domain_urn)]
            ),
        )
        yield mcp

        if self.tags:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=builder.make_tag_urn(tag))
                        for tag in self.tags
                    ]
                ),
            )
            yield mcp

        if self.terms:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(urn=builder.make_term_urn(term))
                        for term in self.terms
                    ],
                    auditStamp=self._mint_auditstamp("yaml"),
                ),
            )
            yield mcp

        if self.owners:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=OwnershipClass(
                    owners=[self._mint_owner(o) for o in self.owners]
                ),
            )
            yield mcp

        # Finally emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn, aspect=StatusClass(removed=False)
        )

    def emit(
        self,
        emitter: Union[DatahubRestEmitter, "DatahubKafkaEmitter"],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the DataProduct entity to Datahub

        :param emitter: Datahub Emitter to emit the event
        :param callback: The callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)

    @classmethod
    def from_yaml(
        cls,
        file: Path,
        graph: DataHubGraph,
    ) -> "DataProduct":
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            orig_dictionary = yaml.load(fp)
            parsed_data_product = DataProduct.parse_obj_allow_extras(orig_dictionary)
            # resolve domains if needed
            domain_registry: DomainRegistry = DomainRegistry(
                cached_domains=[parsed_data_product.domain], graph=graph
            )
            domain_urn = domain_registry.get_domain_urn(parsed_data_product.domain)
            parsed_data_product._resolved_domain_urn = domain_urn
            return parsed_data_product

    @classmethod
    def from_datahub(cls, graph: DataHubGraph, id: str) -> "DataProduct":
        # data_product_dict = graph.get_entity_raw(id)
        data_product_properties: Optional[
            DataProductPropertiesClass
        ] = graph.get_aspect(id, DataProductPropertiesClass)
        domains: Optional[DomainsClass] = graph.get_aspect(id, DomainsClass)
        assert domains, "Data Product must have an associated domain. Found none."
        owners: Optional[OwnershipClass] = graph.get_aspect(id, OwnershipClass)
        yaml_owners: Optional[List[Union[str, DataProduct.Ownership]]] = None
        if owners:
            yaml_owners = []
            for o in owners.owners:
                if o.type == OwnershipTypeClass.TECHNICAL_OWNER:
                    yaml_owners.append(o.owner)
                else:
                    yaml_owners.append(
                        DataProduct.Ownership(id=o.owner, type=str(o.type))
                    )
        return DataProduct(
            id=id,
            display_name=data_product_properties.name
            if data_product_properties
            else None,
            domain=domains.domains[0],
            description=data_product_properties.description
            if data_product_properties
            else None,
            assets=[e.destinationUrn for e in data_product_properties.assets or []]
            if data_product_properties
            else None,
            owners=yaml_owners,
        )

    def _patch_ownership(
        self,
        original_owners: Optional[List[Union[str, DataProduct.Ownership]]],
        original_ownership_list: Optional[List[Any]],
    ) -> Tuple[bool, Optional[List[Any]]]:
        new_owner_type_map = {}
        for new_owner in self.owners or []:
            if isinstance(new_owner, DataProduct.Ownership):
                new_owner_type_map[new_owner.id] = new_owner.type
            else:
                new_owner_type_map[new_owner] = "TECHNICAL_OWNER"
        owners_matched = set()
        patches: Dict[str, Union[dict, list]] = {"ADD": [], "DROP": {}, "REPLACE": {}}

        if original_owners:
            default_ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
            original_owner_urns = set()
            # First figure out which urns to replace or drop
            for i, o in enumerate(original_owners):
                if isinstance(o, str):
                    owner_urn = builder.make_user_urn(o)
                    original_owner_urns.add(owner_urn)
                    if owner_urn in new_owner_type_map:
                        owners_matched.add(owner_urn)
                        if new_owner_type_map[owner_urn] != default_ownership_type:
                            patches["REPLACE"][i] = {
                                "id": o,
                                "type": new_owner_type_map[owner_urn],
                            }
                    else:
                        patches["DROP"][i] = o
                elif isinstance(o, DataProduct.Ownership):
                    owner_urn = builder.make_user_urn(o.id)
                    original_owner_urns.add(owner_urn)
                    if owner_urn in new_owner_type_map:
                        owners_matched.add(owner_urn)
                        if new_owner_type_map[owner_urn] != o.type:
                            patches["REPLACE"][i] = {
                                "id": o,
                                "type": new_owner_type_map[owner_urn],
                            }
                    else:
                        patches["DROP"][i] = o

        # Figure out what if any are new owners to add
        new_owners_to_add = set(o for o in new_owner_type_map) - set(owners_matched)
        if new_owners_to_add:
            for new_owner in new_owners_to_add:
                new_owner_type = new_owner_type_map[new_owner]
                if new_owner_type == OwnershipTypeClass.TECHNICAL_OWNER:
                    patches["ADD"].append(new_owner)  # type: ignore
                else:
                    patches["ADD"].append(DataProduct.Ownership(id=new_owner, type=new_owner_type).dict())  # type: ignore

        mutation_needed = bool(patches["REPLACE"] or patches["DROP"] or patches["ADD"])
        if not mutation_needed:
            return (mutation_needed, original_ownership_list)
        else:
            list_to_manipulate = (
                original_ownership_list if original_ownership_list is not None else []
            )
            for replace_index, replace_value in patches["REPLACE"].items():  # type: ignore
                list_to_manipulate[replace_index] = replace_value

            for drop_index, drop_value in patches["DROP"].items():  # type: ignore
                list_to_manipulate.remove(drop_value)

            for add_value in patches["ADD"]:
                list_to_manipulate.append(add_value)

            return (mutation_needed, list_to_manipulate)

    def patch_yaml(
        self,
        file: Path,
        original_dataproduct: DataProduct,
        output_file: Optional[Path] = None,
    ) -> bool:

        update_needed = False
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            orig_dictionary = yaml.load(fp)
            if original_dataproduct.display_name != self.display_name:
                update_needed = True
                orig_dictionary["display_name"] = self.display_name
            if original_dataproduct.description != self.description:
                update_needed = True
                orig_dictionary["description"] = self.description

            if original_dataproduct.domain != self.domain:
                # we check if the resolved domain urn is the same
                if original_dataproduct._resolved_domain_urn != self.domain:
                    update_needed = True
                    orig_dictionary["domain"] = self.domain

            if set(original_dataproduct.assets) != set(self.assets):
                update_needed = True
                my_asset_urns = [a for a in self.assets]
                assets_to_remove = []
                for asset_urn in orig_dictionary["assets"]:
                    if asset_urn not in my_asset_urns:
                        assets_to_remove.append(asset_urn)
                    else:
                        my_asset_urns.remove(asset_urn)

                for asset_to_remove in assets_to_remove:
                    orig_dictionary["assets"].remove(asset_to_remove)

                for asset_to_add in my_asset_urns:
                    orig_dictionary["assets"].append(asset_to_add)

            (ownership_update_needed, new_ownership_list) = self._patch_ownership(
                original_dataproduct.owners, orig_dictionary.get("owners")
            )
            if ownership_update_needed:
                update_needed = True
                if new_ownership_list:
                    orig_dictionary["owners"] = new_ownership_list
                else:
                    if "owners" in orig_dictionary:
                        # we leave the owners key in, but set it to None (versus empty list) to make the yaml look better
                        orig_dictionary["owners"] = None

            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False

        if update_needed:
            with open(output_file or file, "w") as fp:
                yaml.dump(orig_dictionary, fp)
            return True
        else:
            return False

    def to_yaml(
        self,
        file: Path,
    ) -> None:

        with open(file, "w") as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False
            yaml.dump(self.dict(), fp)

    @staticmethod
    def get_patch_builder(
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> DataProductPatchBuilder:
        return DataProductPatchBuilder(
            urn=urn, system_metadata=system_metadata, audit_header=audit_header
        )


DataProduct.update_forward_refs()
