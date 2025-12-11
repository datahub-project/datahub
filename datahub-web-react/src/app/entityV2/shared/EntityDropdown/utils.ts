/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { GenericEntityProperties } from '@app/entity/shared/types';

import { EntityType, PlatformPrivileges } from '@types';

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
