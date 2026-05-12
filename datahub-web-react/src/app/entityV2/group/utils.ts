import { CorpGroup, Entity, EntityType } from '@src/types.generated';

/**
 * Type guard for groups
 */
export function isCorpGroup(entity?: Entity | null | undefined): entity is CorpGroup {
    return !!entity && entity.type === EntityType.CorpGroup;
}

/**
 * Tooltip copy shown whenever we disable a membership-editing because
 * the group is sourced from an external identity provider (SSO/SCIM/etc.). Kept
 * here so every surface — main pane, sidebar `+`, header lock icon — stays in
 * sync.
 */
export function getExternalGroupMembershipTooltip(externalGroupType: string | undefined): string {
    return `Membership for this group cannot be edited in DataHub as it originates from ${
        externalGroupType || 'outside DataHub'
    }.`;
}
