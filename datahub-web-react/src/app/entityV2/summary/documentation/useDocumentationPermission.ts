import { useEntityData } from '@app/entity/shared/EntityContext';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

export function useDocumentationPermission() {
    const { entityData } = useEntityData();
    const canUpdateGlossaryEntity = useCanUpdateGlossaryEntity();

    // Edit description permission
    const canEditDescription = !!entityData?.privileges?.canEditDescription;
    const canManageSummary = !!entityData?.privileges?.canManageAssetSummary;

    const hasDocumentationPermissions = canEditDescription || canUpdateGlossaryEntity || canManageSummary;

    return hasDocumentationPermissions;
}
