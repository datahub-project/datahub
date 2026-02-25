import { useEffect } from 'react';

import { useGetUsersLazyQuery } from '@src/graphql/user.generated';
import { CorpUser } from '@src/types.generated';

export default function useGetUsers(actors: string[]) {
    const [getUsers, { data, loading }] = useGetUsersLazyQuery({
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        getUsers({ variables: { urns: actors } });
    }, [actors, getUsers]);

    const users = (data?.entities || []) as CorpUser[];

    return {
        users,
        loading,
    };
}
