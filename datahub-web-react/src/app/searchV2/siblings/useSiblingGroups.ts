import { useCallback, useMemo } from 'react';

import { GetSiblingGroupsQuery, useGetSiblingGroupsQuery } from '@graphql/siblings.generated';

/** Siblings listed under each search result before "+ N more". */
export const SIBLINGS_SHOWN_INLINE = 5;

/** The siblingGroups query accepts at most this many urns per call. */
export const MAX_URNS_PER_REQUEST = 50;

type RawSiblingGroup = GetSiblingGroupsQuery['siblingGroups'][number];
type RawSiblingMember = RawSiblingGroup['members'][number];

/** A group member whose entity loaded. */
export type SiblingMemberData = RawSiblingMember & { entity: NonNullable<RawSiblingMember['entity']> };
export type SiblingGroupData = Omit<RawSiblingGroup, 'members'> & { members: SiblingMemberData[] };

/** A member's entity is null when it couldn't be loaded; those members are skipped. */
export function withLoadedMembers(group: RawSiblingGroup): SiblingGroupData {
    return { ...group, members: group.members.filter((member): member is SiblingMemberData => !!member.entity) };
}

/**
 * The sibling groups of the given datasets (the dataset results on the current search page), with
 * the first few members of each, in one request. Nothing is fetched while {@code enabled} is false.
 */
export function useSiblingGroups(datasetUrns: string[], enabled: boolean) {
    // Sorted, so the same datasets in a different order (cached results replaced by fresh ones) are
    // the same request rather than a new one.
    const urns = useMemo(() => Array.from(new Set(datasetUrns)).slice(0, MAX_URNS_PER_REQUEST).sort(), [datasetUrns]);
    const { data, previousData, error, refetch } = useGetSiblingGroupsQuery({
        skip: !enabled || urns.length === 0,
        variables: { input: { urns, start: 0, count: SIBLINGS_SHOWN_INLINE } },
    });
    // While a new request loads, keep showing the last groups, so sibling rows (and an expanded list)
    // don't vanish and come back. Results new to this page simply have no group until it arrives.
    const shown = data ?? (enabled ? previousData : undefined);
    const groups = useMemo(() => {
        const byUrn = new Map<string, SiblingGroupData>();
        (shown?.siblingGroups || []).forEach((group) => byUrn.set(group.urn, withLoadedMembers(group)));
        return byUrn;
    }, [shown]);
    const retry = useCallback(() => {
        refetch().catch(() => undefined); // the hook's error state reports a failure
    }, [refetch]);
    return { groups, error, retry };
}
