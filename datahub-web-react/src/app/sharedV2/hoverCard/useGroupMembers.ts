import { useGetGroupHoverCardMembersQuery } from '@graphql/group.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

/** Enough to recognise the group at a glance without turning the card into a member list. */
export const HOVER_CARD_MEMBER_LIMIT = 8;

export type GroupMembers = {
    total?: number;
    members: CorpUser[];
};

/**
 * Member total, only when a query actually selected it. A missing field is not "0 members". Only the
 * `memberCount` alias counts: a plain `relationships` total depends on whatever types and direction
 * that query asked for, and some group queries select only one kind of membership.
 */
function getSelectedGroupMemberCount(entity: Entity): number | undefined {
    if (entity.type !== EntityType.CorpGroup) return undefined;
    const group = entity as CorpGroup & { memberCount?: { total?: number | null } | null };
    const total = group.memberCount?.total;
    return total == null ? undefined : total;
}

/**
 * Member total and first few members of a group; empty for any other entity. Owner fragments don't
 * select members, since every group owner in a search or list would then cost a graph query, so this
 * fetches them — only once the hover card that calls this is actually open. A total the entity was
 * loaded with stands in until the response arrives.
 */
export default function useGroupMembers(entity: Entity): GroupMembers {
    const isGroup = entity.type === EntityType.CorpGroup;
    const { data } = useGetGroupHoverCardMembersQuery({
        variables: { urn: entity.urn, count: HOVER_CARD_MEMBER_LIMIT },
        skip: !isGroup,
    });

    if (!isGroup) return { members: [] };
    // Only trust a response for this group, whatever Apollo keeps in `data` across skips and urn changes.
    const memberResult = data?.corpGroup?.urn === entity.urn ? data.corpGroup.memberCount : undefined;
    if (!memberResult) return { total: getSelectedGroupMemberCount(entity), members: [] };

    const members = (memberResult.relationships ?? [])
        .map((relationship) => relationship.entity)
        .filter((member): member is CorpUser => member?.__typename === 'CorpUser');
    return { total: memberResult.total ?? undefined, members };
}
