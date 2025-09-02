import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';

import { EntityType } from '@types';

export function useGetLinkPermissions() {
    const { entityData, entityType } = useEntityData();
    const user = useUserContext();

    // Edit links permission
    const canEditLinks = !!entityData?.privileges?.canEditLinks;

    const canManageGlossaries = !!user?.platformPrivileges?.manageGlossaries;
    const canManageChildren = !!entityData?.privileges?.canManageChildren;

    // Manage Glossary or manage children permission for Glossary terms and Glossary nodes
    const canUpdateGlossaryEntity =
        (entityType === EntityType.GlossaryNode || entityType === EntityType.GlossaryTerm) &&
        (canManageGlossaries || canManageChildren);

    const hasLinkPermissions = canEditLinks || canUpdateGlossaryEntity;

    return hasLinkPermissions;
}
