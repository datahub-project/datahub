import { useGetGroupMemberCountQuery } from '@graphql/group.generated';
import { CorpGroup, Entity, EntityType } from '@types';

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
 * Member total of a group, or undefined for any other entity. Owner fragments don't select it, since
 * every group owner in a search or list would then cost a graph query, so when the entity lacks it
 * this fetches it — only once the hover card that calls this is actually open.
 */
export default function useGroupMemberCount(entity: Entity): number | undefined {
    const selectedMemberCount = getSelectedGroupMemberCount(entity);
    const shouldFetch = entity.type === EntityType.CorpGroup && selectedMemberCount === undefined;
    const { data } = useGetGroupMemberCountQuery({ variables: { urn: entity.urn }, skip: !shouldFetch });

    if (!shouldFetch) return selectedMemberCount;
    // Only trust a response for this group, whatever Apollo keeps in `data` across skips and urn changes.
    return data?.corpGroup?.urn === entity.urn ? (data.corpGroup.memberCount?.total ?? undefined) : undefined;
}
