/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useGetUserGroupsUrnsQuery } from '@src/graphql/user.generated';

const NUM_GROUP_URNS_TO_FETCH = 100;

export default function useGetUserGroupUrns(userUrn?: string) {
    const { data, loading } = useGetUserGroupsUrnsQuery({
        variables: { urn: userUrn || '', start: 0, count: NUM_GROUP_URNS_TO_FETCH },
        fetchPolicy: 'cache-first',
        skip: !userUrn,
    });

    const groupUrns: string[] =
        data?.corpUser?.relationships?.relationships?.map((r) => r.entity?.urn || '').filter((u) => !!u) || [];

    return { groupUrns, data, loading };
}
