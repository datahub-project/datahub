import { useGetUserGroupsQuery } from '../../../graphql/user.generated';
import { useUserContext } from '../../context/useUserContext';

const useRelationships = ({ count = 100 } = {}) => {
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    const { data: groupsData } = useGetUserGroupsQuery({
        skip: !authenticatedUserUrn,
        variables: { urn: authenticatedUserUrn as string, start: 0, count },
    });

    const relationships = groupsData?.corpUser?.relationships?.relationships.filter((relationship) => !!relationship);

    const hasRelationships = relationships && relationships.length > 0;

    return { hasRelationships, relationships } as const;
};

export default useRelationships;
