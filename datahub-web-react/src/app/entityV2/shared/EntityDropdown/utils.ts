import { GenericEntityProperties } from '@app/entity/shared/types';

import { EntityPrivileges, EntityType, PlatformPrivileges } from '@types';

export function canShowEditDeprecation(privileges?: EntityPrivileges | null): boolean {
    return privileges?.canEditDeprecation !== false;
}

export type DeprecationMenuAction = 'markDeprecated' | 'editDeprecated' | 'markUnDeprecated';

/**
 * Resolves which deprecation menu actions to surface for an entity, in display order:
 * - not deprecated: only "mark as deprecated"
 * - deprecated: "edit" (only when the user can edit deprecation) followed by "un-deprecate"
 */
export function getDeprecationMenuActions(
    isDeprecated: boolean,
    privileges?: EntityPrivileges | null,
): DeprecationMenuAction[] {
    if (!isDeprecated) {
        return ['markDeprecated'];
    }
    const actions: DeprecationMenuAction[] = [];
    if (canShowEditDeprecation(privileges)) {
        actions.push('editDeprecated');
    }
    actions.push('markUnDeprecated');
    return actions;
}

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
