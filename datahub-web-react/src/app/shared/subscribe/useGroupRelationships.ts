import { useGetUserGroupsQuery } from '../../../graphql/user.generated';
import { useUserContext } from '../../context/useUserContext';

const useGroupRelationships = ({ count = 100 } = {}) => {
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    const { data: groupsData } = useGetUserGroupsQuery({
        skip: !authenticatedUserUrn,
        variables: { urn: authenticatedUserUrn as string, start: 0, count },
    });

    const relationships = groupsData?.corpUser?.relationships?.relationships.filter((relationship) => !!relationship);

    const hasGroupRelationships = relationships && relationships.length > 0;

    return { hasGroupRelationships, relationships } as const;
};

export default useGroupRelationships;
