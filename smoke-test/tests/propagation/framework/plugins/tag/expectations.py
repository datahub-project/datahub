"""Tag-specific expectation classes for tag propagation validation."""

from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field

from tests.propagation.framework.core.expectations import (
    DatasetExpectation,
    SchemaFieldExpectation,
)

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph


# Helper methods for all tag expectations
def get_editable_schema_metadata_aspect(graph_client: "DataHubGraph", urn: str) -> Any:
    """Get the editable schema metadata aspect for the given URN."""
    from datahub.metadata.schema_classes import EditableSchemaMetadataClass

    return graph_client.get_aspect(urn, EditableSchemaMetadataClass)


def get_global_tags_aspect(graph_client: "DataHubGraph", urn: str) -> Any:
    """Get the global tags aspect for the given URN."""
    from datahub.metadata.schema_classes import GlobalTagsClass

    return graph_client.get_aspect(urn, GlobalTagsClass)


class TagPropagationExpectation(SchemaFieldExpectation):
    """Expectation for tag propagation using Pydantic validation."""

    # Tag-specific required fields
    expected_tag_urn: str = Field(..., min_length=1, description="Expected tag URN")

    # Optional fields with defaults
    origin_dataset: str = Field("", description="Origin dataset name")
    origin_field: Optional[str] = Field(None, description="Origin field name")
    is_live: bool = Field(False, description="Whether this is for live testing")
    propagation_found: bool = Field(
        True, description="Whether propagation is expected to be found"
    )
    propagation_source: Optional[str] = Field(
        None, description="Expected propagation source"
    )
    propagation_origin: Optional[str] = Field(
        None, description="Expected propagation origin"
    )
    propagation_via: Optional[str] = Field(
        None, description="Expected propagation via field"
    )
    expected_depth: Optional[int] = Field(
        None, description="Expected propagation depth (1=first hop, 2=second hop, etc.)"
    )
    expected_direction: Optional[str] = Field(
        None, description="Expected propagation direction ('down', 'up', etc.)"
    )
    expected_relationship: Optional[str] = Field(
        None, description="Expected propagation relationship ('lineage', etc.)"
    )

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "tag_propagation"

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Provide human-readable explanation of the expectation."""
        if rollback:
            return (
                f"Expected removal of field tag '{self.expected_tag_urn}' "
                f"from field {self.field_urn}"
            )
        else:
            # Add attribution details for better context
            details = []
            if self.expected_depth is not None:
                details.append(f"depth={self.expected_depth}")
            if self.expected_direction:
                details.append(f"direction={self.expected_direction}")
            if self.propagation_origin:
                details.append(f"origin={self.propagation_origin}")

            detail_str = f" ({', '.join(details)})" if details else ""

            return (
                f"Expected field tag '{self.expected_tag_urn}' "
                f"on field {self.field_urn}{detail_str}"
            )

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if tag propagation expectation is met."""
        validation_data = self._prepare_validation_data(graph_client, rollback)
        if not validation_data:
            return

        self._execute_validation(validation_data, action_urn, rollback, graph_client)

    def _prepare_validation_data(self, graph_client, rollback):
        """Prepare all data needed for validation."""
        from datahub.utilities.urns.urn import Urn

        # Use the field URN directly
        field_urn = self.field_urn
        dataset_urn = Urn.from_string(field_urn).entity_ids[0]
        field_path = Urn.from_string(field_urn).entity_ids[1]

        # Try to get tags from both locations: dataset-level and field-level
        tag_urns = []

        # Check dataset-level tags in EditableSchemaMetadataClass
        field_info = self._get_field_info_from_dataset(
            graph_client, dataset_urn, field_path
        )
        if field_info and field_info.globalTags and field_info.globalTags.tags:
            tag_urns.extend([tag.tag for tag in field_info.globalTags.tags])

        # Check field-level tags in GlobalTagsClass on field URN
        field_level_tags = self._get_field_level_tags(graph_client, field_urn)
        if field_level_tags and field_level_tags.tags:
            tag_urns.extend([tag.tag for tag in field_level_tags.tags])

        # Remove duplicates while preserving order
        unique_tag_urns = list(dict.fromkeys(tag_urns))

        # If no tags found and propagation is expected, that's an error
        if not unique_tag_urns and self.propagation_found and not rollback:
            # Check if we have any metadata at all for better error messages
            has_dataset_metadata = (
                self._get_editable_schema_metadata(graph_client, dataset_urn)
                is not None
            )
            has_field_metadata = field_level_tags is not None

            if not has_dataset_metadata and not has_field_metadata:
                raise AssertionError(
                    f"Expected tag propagation but no metadata found for field {field_urn}"
                )
            else:
                raise AssertionError(
                    f"Expected tag propagation but no tags found for field {field_urn}"
                )

        return {
            "field_info": field_info,  # Keep for compatibility
            "tag_urns": unique_tag_urns,
            "field_urn": field_urn,
        }

    def _execute_validation(self, validation_data, action_urn, rollback, graph_client):
        """Execute validation based on mode."""
        tag_urns = validation_data["tag_urns"]

        tags_propagated_from_action = self._get_tags_from_action(
            validation_data, action_urn, graph_client
        )

        if self.propagation_found and not rollback:
            self._validate_propagation_found(
                action_urn, tags_propagated_from_action, tag_urns
            )
        elif rollback and action_urn:
            self._validate_rollback(tags_propagated_from_action)
        elif not self.propagation_found and not rollback:
            self._validate_no_propagation(tag_urns)

    def _get_field_info_with_validation(
        self, graph_client, dataset_urn, field_path, rollback
    ):
        """Get field info and validate prerequisites."""
        editable_schema_metadata_aspect = get_editable_schema_metadata_aspect(
            graph_client, dataset_urn
        )

        self._validate_schema_aspect_exists(
            editable_schema_metadata_aspect, dataset_urn, rollback
        )
        if not editable_schema_metadata_aspect:
            return None

        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )

        self._validate_field_info_exists(field_info, field_path, rollback)
        if not field_info:
            return None

        self._validate_field_has_tags(field_info, field_path, rollback)
        if not field_info.globalTags or not field_info.globalTags.tags:
            return None

        return field_info

    def _get_field_info_from_dataset(self, graph_client, dataset_urn, field_path):
        """Get field info from dataset-level EditableSchemaMetadataClass (no validation)."""
        editable_schema_metadata_aspect = get_editable_schema_metadata_aspect(
            graph_client, dataset_urn
        )

        if (
            not editable_schema_metadata_aspect
            or not editable_schema_metadata_aspect.editableSchemaFieldInfo
        ):
            return None

        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )
        return field_info

    def _get_field_level_tags(self, graph_client, field_urn):
        """Get GlobalTagsClass directly from field URN."""
        return get_global_tags_aspect(graph_client, field_urn)

    def _get_editable_schema_metadata(self, graph_client, dataset_urn):
        """Get EditableSchemaMetadataClass from dataset URN."""
        return get_editable_schema_metadata_aspect(graph_client, dataset_urn)

    def _validate_schema_aspect_exists(
        self, editable_schema_metadata_aspect, dataset_urn, rollback
    ):
        """Validate that schema aspect exists when expected."""
        if (
            not editable_schema_metadata_aspect
            and self.propagation_found
            and not rollback
        ):
            raise AssertionError(
                f"Expected tag propagation but no editable schema metadata found for dataset {dataset_urn}"
            )

    def _validate_field_info_exists(self, field_info, field_path, rollback):
        """Validate that field info exists when expected."""
        if not field_info and self.propagation_found and not rollback:
            raise AssertionError(
                f"Expected tag propagation but field {field_path} not found in editable schema"
            )

    def _validate_field_has_tags(self, field_info, field_path, rollback):
        """Validate that field has tags when expected."""
        if (
            (not field_info.globalTags or not field_info.globalTags.tags)
            and self.propagation_found
            and not rollback
        ):
            raise AssertionError(
                f"Expected tag propagation but field {field_path} has no global tags"
            )

    def _get_tags_from_action(self, validation_data, action_urn, graph_client):
        """Get tags that were propagated by the specific action."""
        if not action_urn:
            return []

        tags_from_action = []
        field_info = validation_data.get("field_info")
        field_urn = validation_data.get("field_urn")

        # Check dataset-level tags (field_info from EditableSchemaMetadataClass)
        if field_info and field_info.globalTags and field_info.globalTags.tags:
            tags_from_action.extend(
                [
                    x
                    for x in field_info.globalTags.tags
                    if hasattr(x, "attribution")
                    and x.attribution
                    and x.attribution.source == action_urn
                ]
            )

        # Check field-level tags (GlobalTagsClass on field URN)
        if field_urn:
            field_level_tags = self._get_field_level_tags(graph_client, field_urn)
            if field_level_tags and field_level_tags.tags:
                tags_from_action.extend(
                    [
                        x
                        for x in field_level_tags.tags
                        if hasattr(x, "attribution")
                        and x.attribution
                        and x.attribution.source == action_urn
                    ]
                )

        return tags_from_action

    def _validate_propagation_found(
        self, action_urn, tags_propagated_from_action, tag_urns
    ):
        """Validate that expected propagation occurred."""
        if action_urn:
            expected_tag = next(
                (
                    x
                    for x in tags_propagated_from_action
                    if x.tag == self.expected_tag_urn
                ),
                None,
            )

            if not expected_tag:
                raise AssertionError(
                    f"Expected tag {self.expected_tag_urn} not found in propagated tags from action"
                )

            self.check_attribution(
                action_urn,
                expected_tag.attribution,
                self.propagation_source,
                self.propagation_origin,
                self.propagation_via,
                self.expected_depth,
                self.expected_direction,
                self.expected_relationship,
            )
        else:
            # Simple check without action attribution
            if self.expected_tag_urn not in tag_urns:
                raise AssertionError(
                    f"Expected to have tag '{self.expected_tag_urn}' on {self.field_urn} "
                    f"but found tags: {tag_urns}"
                )

    def _validate_rollback(self, tags_propagated_from_action):
        """Validate that tags were properly rolled back."""
        rollback_tags = [
            x for x in tags_propagated_from_action if x.tag == self.expected_tag_urn
        ]
        if rollback_tags:
            raise AssertionError("Propagated tag should have been rolled back")

    def _validate_no_propagation(self, tag_urns):
        """Validate that no unexpected propagation occurred."""
        if self.expected_tag_urn in tag_urns:
            raise AssertionError(f"Unexpected tag found: {self.expected_tag_urn}")


class DatasetTagPropagationExpectation(DatasetExpectation):
    """Expectation for dataset-level tag propagation using Pydantic validation."""

    # Tag-specific required fields
    expected_tag_urn: str = Field(..., min_length=1, description="Expected tag URN")

    # Optional fields with defaults
    origin_dataset: str = Field("", description="Origin dataset name")
    is_live: bool = Field(False, description="Whether this is for live testing")
    propagation_source: Optional[str] = Field(
        None, description="Expected propagation source"
    )
    expected_depth: Optional[int] = Field(
        None, description="Expected propagation depth (1=first hop, 2=second hop, etc.)"
    )
    expected_direction: Optional[str] = Field(
        None, description="Expected propagation direction ('down', 'up', etc.)"
    )
    expected_relationship: Optional[str] = Field(
        None, description="Expected propagation relationship ('lineage', etc.)"
    )

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "dataset_tag_propagation"

    def explain(self, rollback: bool = False) -> Optional[str]:
        """Provide human-readable explanation of the expectation."""
        if rollback:
            return (
                f"Expected removal of dataset tag '{self.expected_tag_urn}' "
                f"from dataset {self.dataset_urn}"
            )
        else:
            # Add attribution details for better context
            details = []
            if self.expected_depth is not None:
                details.append(f"depth={self.expected_depth}")
            if self.expected_direction:
                details.append(f"direction={self.expected_direction}")
            if self.origin_dataset:
                details.append(f"origin={self.origin_dataset}")

            detail_str = f" ({', '.join(details)})" if details else ""

            return (
                f"Expected dataset tag '{self.expected_tag_urn}' "
                f"on dataset {self.dataset_urn}{detail_str}"
            )

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if dataset tag propagation expectation is met."""
        dataset_urn = self.dataset_urn
        global_tags_aspect = get_global_tags_aspect(graph_client, dataset_urn)

        tag_urns = (
            [tag.tag for tag in global_tags_aspect.tags]
            if global_tags_aspect and global_tags_aspect.tags
            else []
        )

        if rollback:
            # During rollback, expect the tag to NOT be present
            assert self.expected_tag_urn not in tag_urns, (
                f"Expected tag '{self.expected_tag_urn}' to be removed from dataset {self.dataset_urn} "
                f"after rollback, but found tags: {tag_urns}"
            )
        else:
            # Normal mode: expect the tag to be present
            assert global_tags_aspect is not None, (
                f"Expected to have global tags aspect on dataset {self.dataset_urn} but it was missing"
            )
            assert self.expected_tag_urn in tag_urns, (
                f"Expected to have tag '{self.expected_tag_urn}' on dataset {self.dataset_urn} "
                f"but found tags: {tag_urns}"
            )

            # If action_urn is provided and we have validation fields, check attribution
            if action_urn and (
                self.propagation_source
                or self.expected_depth is not None
                or self.expected_direction
                or self.expected_relationship
            ):
                # Find the specific tag with attribution
                expected_tag = next(
                    (
                        tag
                        for tag in global_tags_aspect.tags
                        if tag.tag == self.expected_tag_urn
                        and hasattr(tag, "attribution")
                        and tag.attribution
                    ),
                    None,
                )

                if not expected_tag:
                    raise AssertionError(
                        f"Expected tag '{self.expected_tag_urn}' with attribution not found on dataset {self.dataset_urn}"
                    )

                # Use the base check_attribution method for detailed validation
                self.check_attribution(
                    action_urn,
                    expected_tag.attribution,
                    expected_source=self.propagation_source,
                    expected_depth=self.expected_depth,
                    expected_direction=self.expected_direction,
                    expected_relationship=self.expected_relationship,
                )


class NoTagPropagationExpectation(SchemaFieldExpectation):
    """Expectation for no tag propagation using Pydantic validation."""

    # No additional fields needed beyond base schema field expectation

    # Optional fields
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "no_tag_propagation"

    def explain(self, rollback: bool = False) -> str:
        """Return a human-friendly explanation of what this expectation does."""
        # For "no propagation" expectations, rollback mode doesn't change the meaning
        explanation = f"🚫 Expect NO tags to be propagated to field '{self.field_urn}'"

        if self.is_live:
            explanation += " (during live propagation)"

        return explanation

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check that no tag propagation occurred."""
        # Get dataset URN from field URN - similar to term propagation pattern
        from datahub.utilities.urns.urn import Urn

        # Use the field URN directly
        field_urn = self.field_urn
        dataset_urn = Urn.from_string(field_urn).entity_ids[0]
        field_path = Urn.from_string(field_urn).entity_ids[1]

        editable_schema_metadata_aspect = get_editable_schema_metadata_aspect(
            graph_client, dataset_urn
        )

        if not editable_schema_metadata_aspect:
            # No editable schema metadata means no tags - this is expected for no propagation
            return

        # Find the specific field in editableSchemaFieldInfo
        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )

        if not field_info:
            # Field not found in editable schema means no tags - this is expected
            return

        # Check if field has tags
        if field_info.globalTags and field_info.globalTags.tags:
            tag_urns = [tag.tag for tag in field_info.globalTags.tags]
            raise AssertionError(
                f"Expected no tags on {self.field_urn} but found tags: {tag_urns}"
            )
