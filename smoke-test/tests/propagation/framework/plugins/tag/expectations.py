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

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "tag_propagation"

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

        self._execute_validation(validation_data, action_urn, rollback)

    def _prepare_validation_data(self, graph_client, rollback):
        """Prepare all data needed for validation."""
        from datahub.utilities.urns.urn import Urn

        field_urn = self.get_urn(self.platform, self.dataset_name, self.field_name)
        dataset_urn = Urn.from_string(field_urn).entity_ids[0]
        field_path = Urn.from_string(field_urn).entity_ids[1]

        field_info = self._get_field_info_with_validation(
            graph_client, dataset_urn, field_path, rollback
        )
        if not field_info:
            return None

        return {
            "field_info": field_info,
            "tag_urns": [tag.tag for tag in field_info.globalTags.tags],
        }

    def _execute_validation(self, validation_data, action_urn, rollback):
        """Execute validation based on mode."""
        field_info = validation_data["field_info"]
        tag_urns = validation_data["tag_urns"]

        tags_propagated_from_action = self._get_tags_from_action(field_info, action_urn)

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

    def _get_tags_from_action(self, field_info, action_urn):
        """Get tags that were propagated by the specific action."""
        if not action_urn:
            return []
        return [
            x
            for x in field_info.globalTags.tags
            if hasattr(x, "attribution")
            and x.attribution
            and x.attribution.source == action_urn
        ]

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
            )
        else:
            # Simple check without action attribution
            if self.expected_tag_urn not in tag_urns:
                raise AssertionError(
                    f"Expected to have tag '{self.expected_tag_urn}' on {self.dataset_name}.{self.field_name} "
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

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "dataset_tag_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check if dataset tag propagation expectation is met."""
        dataset_urn = self.get_urn(self.platform, self.dataset_name)
        global_tags_aspect = get_global_tags_aspect(graph_client, dataset_urn)

        assert global_tags_aspect is not None, (
            f"Expected to have global tags aspect on dataset {self.dataset_name} but it was missing"
        )

        tag_urns = (
            [tag.tag for tag in global_tags_aspect.tags]
            if global_tags_aspect.tags
            else []
        )
        assert self.expected_tag_urn in tag_urns, (
            f"Expected to have tag '{self.expected_tag_urn}' on dataset {self.dataset_name} "
            f"but found tags: {tag_urns}"
        )


class NoTagPropagationExpectation(SchemaFieldExpectation):
    """Expectation for no tag propagation using Pydantic validation."""

    # No additional fields needed beyond base schema field expectation

    # Optional fields
    is_live: bool = Field(False, description="Whether this is for live testing")

    def get_expectation_type(self) -> str:
        """Return the expectation type identifier."""
        return "no_tag_propagation"

    def check_expectation(
        self,
        graph_client: "DataHubGraph",
        action_urn: Optional[str] = None,
        rollback: bool = False,
    ) -> None:
        """Check that no tag propagation occurred."""
        # Get dataset URN from field URN - similar to term propagation pattern
        from datahub.utilities.urns.urn import Urn

        field_urn = self.get_urn(self.platform, self.dataset_name, self.field_name)
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
                f"Expected no tags on {self.dataset_name}.{self.field_name} "
                f"but found tags: {tag_urns}"
            )
