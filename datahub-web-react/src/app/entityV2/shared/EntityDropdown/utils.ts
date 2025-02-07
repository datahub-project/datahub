import { EntityType, PlatformPrivileges } from '../../../../types.generated';
import { GenericEntityProperties } from '../../../entity/shared/types';

export function isDeleteDisabled(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
    platformPrivileges: PlatformPrivileges | null | undefined,
) {
    if (entityType === EntityType.GlossaryTerm || entityType === EntityType.GlossaryNode) {
        const entityHasChildren = !!entityData?.children?.total;
        const canManageGlossaryEntity = !!entityData?.privileges?.canManageEntity;
        const canDeleteGlossaryEntity = !entityHasChildren && canManageGlossaryEntity;
        return !canDeleteGlossaryEntity;
    }
    if (entityType === EntityType.DataProduct) {
        return false; // TODO: update with permissions
    }
    if (entityType === EntityType.Domain) {
        const entityHasChildren = !!entityData?.children?.total;
        const canManageDomains = !!platformPrivileges?.manageDomains;
        const canDeleteDomainEntity = !entityHasChildren && canManageDomains;
        return !canDeleteDomainEntity;
    }
    return false;
}

export function isMoveDisabled(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
    platformPrivileges: PlatformPrivileges | null | undefined,
) {
    if (entityType === EntityType.GlossaryTerm || entityType === EntityType.GlossaryNode) {
        const canManageGlossaryEntity = !!entityData?.privileges?.canManageEntity;
        return !canManageGlossaryEntity;
    }
    if (entityType === EntityType.Domain) {
        const canManageDomains = !!platformPrivileges?.manageDomains;
        return !canManageDomains;
    }
    return false;
}

export function shouldDisplayChildDeletionWarning(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
    platformPrivileges: PlatformPrivileges | null | undefined,
) {
    if (entityType === EntityType.GlossaryTerm || entityType === EntityType.GlossaryNode) {
        const entityHasChildren = !!entityData?.children?.total;
        const canManageGlossaryEntity = !!entityData?.privileges?.canManageEntity;
        const hasTooltip = entityHasChildren && canManageGlossaryEntity;
        return hasTooltip;
    }
    if (entityType === EntityType.Domain) {
        const entityHasChildren = !!entityData?.children?.total;
        const canManageDomains = !!platformPrivileges?.manageDomains;
        const hasTooltip = entityHasChildren && canManageDomains;
        return hasTooltip;
    }
    return false;
}
