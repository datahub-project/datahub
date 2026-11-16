from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch


class DocumentPatchBuilder(
    HasOwnershipPatch, HasTagsPatch, HasTermsPatch, MetadataPatchProposal
):
    """Patch document associations without replacing their containing aspects."""
