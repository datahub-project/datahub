from typing import List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    FormInfoClass as FormInfo,
    FormPromptClass,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
)
from datahub.specific.ownership import OwnershipPatchHelper
from datahub.utilities.urns.urn import Urn


class FormPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.ownership_patch_helper = OwnershipPatchHelper(self)

    def add_owner(self, owner: Owner) -> "FormPatchBuilder":
        self.ownership_patch_helper.add_owner(owner)
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "FormPatchBuilder":
        """
        param: owner_type is optional
        """
        self.ownership_patch_helper.remove_owner(owner, owner_type)
        return self

    def set_owners(self, owners: List[Owner]) -> "FormPatchBuilder":
        self.ownership_patch_helper.set_owners(owners)
        return self

    def set_name(self, name: Optional[str] = None) -> "FormPatchBuilder":
        if name is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path="/name",
                value=name,
            )
        return self

    def set_description(self, description: Optional[str] = None) -> "FormPatchBuilder":
        if description is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path="/description",
                value=description,
            )
        return self

    def set_type(self, type: Optional[str] = None) -> "FormPatchBuilder":
        if type is not None:
            self._add_patch(
                FormInfo.ASPECT_NAME,
                "add",
                path="/type",
                value=type,
            )
        return self

    def add_prompt(self, prompt: FormPromptClass) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=f"/prompts/{self.quote(prompt.id)}",
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
            path=f"/prompts/{self.quote(prompt_id)}",
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
            path="/actors/owners",
            value=is_ownership,
        )
        return self

    def add_assigned_user(self, user_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=f"/actors/users/{self.quote(str(user_urn))}",
            value=user_urn,
        )
        return self

    def remove_assigned_user(self, user_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "remove",
            path=f"/actors/users/{self.quote(str(user_urn))}",
            value=user_urn,
        )
        return self

    def add_assigned_group(self, group_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "add",
            path=f"/actors/groups/{self.quote(str(group_urn))}",
            value=group_urn,
        )
        return self

    def remove_assigned_group(self, group_urn: Union[str, Urn]) -> "FormPatchBuilder":
        self._add_patch(
            FormInfo.ASPECT_NAME,
            "remove",
            path=f"/actors/groups/{self.quote(str(group_urn))}",
            value=group_urn,
        )
        return self
