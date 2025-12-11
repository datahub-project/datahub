/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
