import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { useCanUpdateGlossaryEntity } from '@app/entityV2/summary/shared/useCanUpdateGlossaryEntity';

export function useLinkPermission() {
    const { entityData } = useEntityData();
    const canUpdateGlossaryEntity = useCanUpdateGlossaryEntity();

    // Edit links permission
    const canEditLinks = !!entityData?.privileges?.canEditLinks;
    const canManageSummary = !!entityData?.privileges?.canManageAssetSummary;

    const hasLinkPermissions = canEditLinks || canUpdateGlossaryEntity || canManageSummary;

    return hasLinkPermissions;
}
