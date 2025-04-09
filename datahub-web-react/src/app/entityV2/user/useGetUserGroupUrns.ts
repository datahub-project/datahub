import { useGetUserGroupsUrnsQuery } from '@src/graphql/user.generated';

const NUM_GROUP_URNS_TO_FETCH = 100;

export default function useGetUserGroupUrns(userUrn: string) {
    const { data, loading } = useGetUserGroupsUrnsQuery({
        variables: { urn: userUrn, start: 0, count: NUM_GROUP_URNS_TO_FETCH },
        fetchPolicy: 'cache-first',
    });

    const groupUrns: string[] =
        data?.corpUser?.relationships?.relationships.map((r) => r.entity?.urn || '').filter((u) => !!u) || [];

    return { groupUrns, data, loading };
}
