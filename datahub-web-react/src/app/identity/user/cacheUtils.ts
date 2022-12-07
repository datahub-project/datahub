import { ListUsersDocument, ListUsersQuery } from '../../../graphql/user.generated';

export const removeUserFromListUsesCache = (urn, client, page, pageSize, query) => {
    const currData: ListUsersQuery | null = client.readQuery({
        query: ListUsersDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
    });

    const newUsers = [...(currData?.listUsers?.users || []).filter((source) => source.urn !== urn)];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListUsersDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
        data: {
            listUsers: {
                start: currData?.listUsers?.start || 0,
                count: (currData?.listUsers?.count || 1) - 1,
                total: (currData?.listUsers?.total || 1) - 1,
                users: newUsers,
            },
        },
    });
};

const DEFAULT_LIST_SIZE = 25;

export const clearUserListCache = (client) => {
    client.cache.evict({
        input: {
            start: 0,
            count: DEFAULT_LIST_SIZE,
            query: undefined,
        },
    });
};
