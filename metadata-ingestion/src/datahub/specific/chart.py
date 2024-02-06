import time
from typing import Dict, List, Optional, TypeVar, Union
from urllib.parse import quote

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChartInfoClass as ChartInfo,
    EdgeClass as Edge,
    GlobalTagsClass as GlobalTags,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
)
from datahub.specific.custom_properties import CustomPropertiesPatchHelper
from datahub.specific.ownership import OwnershipPatchHelper
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

T = TypeVar("T", bound=MetadataPatchProposal)


class ChartPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        """
        Initializes a ChartPatchBuilder instance.

        Args:
            urn: The URN of the chart
            system_metadata: The system metadata of the chart (optional).
            audit_header: The Kafka audit header of the chart (optional).
        """
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.custom_properties_patch_helper = CustomPropertiesPatchHelper(
            self, ChartInfo.ASPECT_NAME
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def _mint_auditstamp(self, message: Optional[str] = None) -> AuditStampClass:
        """
        Creates an AuditStampClass instance with the current timestamp and other default values.

        Args:
            message: The message associated with the audit stamp (optional).

        Returns:
            An instance of AuditStampClass.
        """
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    def _ensure_urn_type(
        self, entity_type: str, edges: List[Edge], context: str
    ) -> None:
        """
        Ensures that the destination URNs in the given edges have the specified entity type.

        Args:
            entity_type: The entity type to check against.
            edges: A list of Edge objects.
            context: The context or description of the operation.

        Raises:
            ValueError: If any of the destination URNs is not of the specified entity type.
        """
        for e in edges:
            urn = Urn.create_from_string(e.destinationUrn)
            if not urn.get_type() == entity_type:
                raise ValueError(
                    f"{context}: {e.destinationUrn} is not of type {entity_type}"
                )

    def add_owner(self, owner: Owner) -> "ChartPatchBuilder":
        """
        Adds an owner to the ChartPatchBuilder.

        Args:
            owner: The Owner object to add.

        Returns:
            The ChartPatchBuilder instance.
        """
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "ChartPatchBuilder":
        """
        Removes an owner from the ChartPatchBuilder.

        Args:
            owner: The owner to remove.
            owner_type: The ownership type of the owner (optional).

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            `owner_type` is optional.
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "ChartPatchBuilder":
        """
        Sets the owners of the ChartPatchBuilder.

        Args:
            owners: A list of Owner objects.

        Returns:
            The ChartPatchBuilder instance.
        """
        self.ownership_patch_helper.set_owners(owners)
        return self

    def add_input_edge(self, input: Union[Edge, Urn, str]) -> "ChartPatchBuilder":
        """
        Adds an input to the ChartPatchBuilder.

        Args:
            input: The input, which can be an Edge object, Urn object, or a string.

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            If `input` is an Edge object, it is used directly. If `input` is a Urn object or string,
            it is converted to an Edge object and added with default audit stamps.
        """
        if isinstance(input, Edge):
            input_urn: str = input.destinationUrn
            input_edge: Edge = input
        elif isinstance(input, (Urn, str)):
            input_urn = str(input)

            input_edge = Edge(
                destinationUrn=input_urn,
                created=self._mint_auditstamp(),
                lastModified=self._mint_auditstamp(),
            )

        self._ensure_urn_type("dataset", [input_edge], "add_dataset")
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path=f"/inputEdges/{quote(input_urn, safe='')}",
            value=input_urn,
        )
        return self

    def remove_input_edge(self, input: Union[str, Urn]) -> "ChartPatchBuilder":
        """
        Removes an input from the ChartPatchBuilder.

        Args:
            input: The input to remove, specified as a string or Urn object.

        Returns:
            The ChartPatchBuilder instance.
        """
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "remove",
            path=f"/inputEdges/{input}",
            value={},
        )
        return self

    def set_input_edges(self, inputs: List[Edge]) -> "ChartPatchBuilder":
        """
        Sets the input edges for the ChartPatchBuilder.

        Args:
            inputs: A list of Edge objects representing the input edges.

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            This method replaces all existing inputs with the given inputs.
        """
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path="/inputEdges",
            value=inputs,
        )
        return self

    def add_tag(self, tag: Tag) -> "ChartPatchBuilder":
        """
        Adds a tag to the ChartPatchBuilder.

        Args:
            tag: The Tag object representing the tag to be added.

        Returns:
            The ChartPatchBuilder instance.
        """
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=f"/tags/{tag.tag}", value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> "ChartPatchBuilder":
        """
        Removes a tag from the ChartPatchBuilder.

        Args:
            tag: The tag to remove, specified as a string or Urn object.

        Returns:
            The ChartPatchBuilder instance.
        """
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=f"/tags/{tag}", value={})
        return self

    def add_term(self, term: Term) -> "ChartPatchBuilder":
        """
        Adds a glossary term to the ChartPatchBuilder.

        Args:
            term: The Term object representing the glossary term to be added.

        Returns:
            The ChartPatchBuilder instance.
        """
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "add", path=f"/terms/{term.urn}", value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> "ChartPatchBuilder":
        """
        Removes a glossary term from the ChartPatchBuilder.

        Args:
            term: The term to remove, specified as a string or Urn object.

        Returns:
            The ChartPatchBuilder instance.
        """
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = "urn:li:glossaryTerm:" + term
        self._add_patch(
            GlossaryTerms.ASPECT_NAME, "remove", path=f"/terms/{term}", value={}
        )
        return self

    def set_custom_properties(
        self, custom_properties: Dict[str, str]
    ) -> "ChartPatchBuilder":
        """
        Sets the custom properties for the ChartPatchBuilder.

        Args:
            custom_properties: A dictionary containing the custom properties to be set.

        Returns:
            The ChartPatchBuilder instance.

        Notes:
            This method replaces all existing custom properties with the given dictionary.
        """
        self._add_patch(
            ChartInfo.ASPECT_NAME,
            "add",
            path="/customProperties",
            value=custom_properties,
        )
        return self

    def add_custom_property(self, key: str, value: str) -> "ChartPatchBuilder":
        """
        Adds a custom property to the ChartPatchBuilder.

        Args:
            key: The key of the custom property.
            value: The value of the custom property.

        Returns:
            The ChartPatchBuilder instance.
        """
        self.custom_properties_patch_helper.add_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> "ChartPatchBuilder":
        """
        Removes a custom property from the ChartPatchBuilder.

        Args:
            key: The key of the custom property to remove.

        Returns:
            The ChartPatchBuilder instance.
        """
        self.custom_properties_patch_helper.remove_property(key)
        return self
