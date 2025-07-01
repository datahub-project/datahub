from typing import List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    FormInfoClass as FormInfo,
    FormPromptClass,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.utilities.urns.urn import Urn


class FormPatchBuilder(HasOwnershipPatch, MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    def set_name(self, name: Optional[str] = None) -> "FormPatchBuilder":
        if name is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path=("name",),
                value=name,
            )
        return self

    def set_description(self, description: Optional[str] = None) -> "FormPatchBuilder":
        if description is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path=("description",),
                value=description,
            )
        return self

    def set_type(self, type: Optional[str] = None) -> "FormPatchBuilder":
        if type is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path=("type",),
                value=type,
            )
        return self

    def add_prompt(self, prompt: FormPromptClass) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=("prompts", prompt.id),
            value=prompt,
        )
        return self

    def add_prompts(self, prompts: List[FormPromptClass]) -> "FormPatchBuilder":
        for prompt in prompts:
            self.add_prompt(prompt)
        return self

    def remove_prompt(self, prompt_id: str) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "remove",
            path=("prompts", prompt_id),
            value=prompt_id,
        )
        return self

    def remove_prompts(self, prompt_ids: List[str]) -> "FormPatchBuilder":
        for prompt_id in prompt_ids:
            self.remove_prompt(prompt_id)
        return self

    def set_ownership_form(self, is_ownership: bool) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=("actors", "owners"),
            value=is_ownership,
        )
        return self

    def add_assigned_user(self, user_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=("actors", "users", user_urn),
            value=user_urn,
        )
        return self

    def remove_assigned_user(self, user_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "remove",
            path=("actors", "users", user_urn),
            value=user_urn,
        )
        return self

    def add_assigned_group(self, group_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=("actors", "groups", group_urn),
            value=group_urn,
        )
        return self

    def remove_assigned_group(self, group_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "remove",
            path=("actors", "groups", group_urn),
            value=group_urn,
        )
        return self
