import { useUserContext } from '@app/context/useUserContext';

import { useGetOwnedGroupsQuery } from '@graphql/group.generated';
import { useGetUserGroupsQuery } from '@graphql/user.generated';

const useGroupRelationships = ({ count = 100 } = {}) => {
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    const { data: groupsData, refetch: refetchGroups } = useGetUserGroupsQuery({
        skip: !authenticatedUserUrn,
        variables: { urn: authenticatedUserUrn as string, start: 0, count },
    });
    const { data: ownedGroupsData, refetch: refetchOwnedGroups } = useGetOwnedGroupsQuery({
        skip: !authenticatedUserUrn,
        variables: { userUrn: authenticatedUserUrn || '', start: 0, count },
    });

    const relationships = groupsData?.corpUser?.relationships?.relationships?.filter((relationship) => !!relationship);

    const ownedGroupSearchResults = ownedGroupsData?.search?.searchResults;

    const hasGroupRelationships =
        (relationships && relationships.length > 0) || (ownedGroupSearchResults && ownedGroupSearchResults.length > 0);

    const refetch = () => {
        return Promise.all([refetchGroups(), refetchOwnedGroups()]);
    };

    return { hasGroupRelationships, relationships, ownedGroupSearchResults, refetch } as const;
};

export default useGroupRelationships;
