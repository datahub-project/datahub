import { GenericEntityProperties } from '@app/entity/shared/types';

import { EntityPrivileges, EntityType, LifecycleStageType, PlatformPrivileges } from '@types';

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

const NO_STAGE_SENTINEL = 'urn:li:lifecycleStageType:__NONE__';

type SelectableStage = Pick<LifecycleStageType, 'urn' | 'name' | 'userAssignable' | 'allowedPreviousStages'>;

/**
 * Stages the user may move to from the current one, mirroring the server's entry rules:
 * a null list accepts any stage, otherwise the current stage (or __NONE__) must be listed.
 */
export function getSelectableStages<T extends SelectableStage>(stages: T[], currentStageUrn?: string | null): T[] {
    const currentKey = currentStageUrn ?? NO_STAGE_SENTINEL;
    return stages.filter(
        (stage) =>
            stage.userAssignable &&
            stage.urn !== currentStageUrn &&
            (stage.allowedPreviousStages == null || stage.allowedPreviousStages.includes(currentKey)),
    );
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

const TRAILING_NUMBER = /^(.*?)(\d+)$/;

/**
 * Whether the user may create a new version of this glossary term: versioning is on, they hold the
 * same right createGlossaryTermVersion checks (canManageEntity is computed from the parent node for
 * glossary terms), and this is the latest version or the term is not versioned yet.
 */
export function canCreateTermVersion(versioningEnabled: boolean, entityData?: GenericEntityProperties | null): boolean {
    if (!versioningEnabled || !entityData?.privileges?.canManageEntity) return false;
    return !entityData.versionProperties || !!entityData.versionProperties.isLatest;
}

/** Deleting a glossary term version removes only that version; the rest of its version set stays. */
export function isGlossaryTermVersion(entityType: EntityType, entityData?: GenericEntityProperties | null): boolean {
    return entityType === EntityType.GlossaryTerm && !!entityData?.versionProperties;
}

/** Increments the label's trailing number (v9 → v10, 2024-Q1 → 2024-Q2); an unversioned term suggests v2. */
export function suggestNextVersionLabel(currentLabel?: string | null): string {
    if (!currentLabel) return 'v2';
    const match = TRAILING_NUMBER.exec(currentLabel.trim());
    return match ? `${match[1]}${Number(match[2]) + 1}` : '';
}
