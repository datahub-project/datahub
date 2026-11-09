import { useCallback, useMemo } from 'react';

import { GetSiblingGroupsQuery, useGetSiblingGroupsQuery } from '@graphql/siblings.generated';

/** Siblings listed under each search result before "+ N more". */
export const SIBLINGS_SHOWN_INLINE = 5;

/** The siblingGroups query accepts at most this many urns per call. */
export const MAX_URNS_PER_REQUEST = 50;

export type SiblingGroupData = GetSiblingGroupsQuery['siblingGroups'][number];
export type SiblingMemberData = SiblingGroupData['members'][number];

/**
 * The sibling groups of the given datasets (the dataset results on the current search page), with
 * the first few members of each, in one request. Nothing is fetched while {@code enabled} is false.
 */
export function useSiblingGroups(datasetUrns: string[], enabled: boolean) {
    const urns = useMemo(() => Array.from(new Set(datasetUrns)).slice(0, MAX_URNS_PER_REQUEST), [datasetUrns]);
    const { data, error, refetch } = useGetSiblingGroupsQuery({
        skip: !enabled || urns.length === 0,
        variables: { input: { urns, start: 0, count: SIBLINGS_SHOWN_INLINE } },
    });
    const groups = useMemo(() => {
        const byUrn = new Map<string, SiblingGroupData>();
        (data?.siblingGroups || []).forEach((group) => byUrn.set(group.urn, group));
        return byUrn;
    }, [data]);
    const retry = useCallback(() => {
        refetch().catch(() => undefined); // the hook's error state reports a failure
    }, [refetch]);
    return { groups, error, retry };
}
