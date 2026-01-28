import { ApolloClient } from '@apollo/client';

import { ListServiceAccountsDocument, ListServiceAccountsQuery } from '@graphql/auth.generated';
import { EntityType } from '@types';

export const DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE = 10;

/**
 * Data structure for a newly created service account from the mutation response.
 */
export interface CreatedServiceAccountData {
    urn: string;
    name: string;
    displayName?: string | null;
    description?: string | null;
    createdBy?: string | null;
    createdAt?: number | null;
}

/**
 * Creates a full service account object that matches the cache structure.
 */
function createFullServiceAccount(newServiceAccount: CreatedServiceAccountData) {
    return {
        __typename: 'ServiceAccount',
        urn: newServiceAccount.urn,
        type: EntityType.CorpUser,
        name: newServiceAccount.name,
        displayName: newServiceAccount.displayName || null,
        description: newServiceAccount.description || null,
        createdBy: newServiceAccount.createdBy || null,
        createdAt: newServiceAccount.createdAt || null,
        updatedAt: null,
        // Match the structure of the roles field from the GraphQL query
        roles: {
            __typename: 'EntityRelationshipsResult',
            start: 0,
            count: 1,
            total: 0,
            relationships: [],
        },
    };
}

/**
 * Adds a newly created service account to the Apollo cache.
 * This updates the cached listServiceAccounts query to include the new service account at the top.
 *
 * @param client - The Apollo client instance
 * @param newServiceAccount - The newly created service account data from the mutation response
 * @param pageSize - The page size used for the list query (defaults to DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE)
 */
export function addServiceAccountToListCache(
    client: ApolloClient<any>,
    newServiceAccount: CreatedServiceAccountData,
    pageSize: number = DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE,
): void {
    const currData: ListServiceAccountsQuery | null = client.readQuery({
        query: ListServiceAccountsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
    });

    if (currData === null) {
        // If there's no cached data, the first load has not occurred. Let it occur naturally.
        return;
    }

    const existingServiceAccounts = currData?.listServiceAccounts?.serviceAccounts || [];
    const newServiceAccountFull = createFullServiceAccount(newServiceAccount);
    const newServiceAccounts = [newServiceAccountFull, ...existingServiceAccounts];

    client.writeQuery({
        query: ListServiceAccountsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
            },
        },
        data: {
            listServiceAccounts: {
                __typename: 'ListServiceAccountsResult',
                start: currData?.listServiceAccounts?.start || 0,
                count: (currData?.listServiceAccounts?.count || 0) + 1,
                total: (currData?.listServiceAccounts?.total || 0) + 1,
                serviceAccounts: newServiceAccounts,
            },
        },
    });
}

/**
 * Removes a service account from the Apollo cache.
 * This updates the cached listServiceAccounts query to exclude the deleted service account.
 *
 * @param client - The Apollo client instance
 * @param serviceAccountUrn - The URN of the service account to remove
 * @param page - The current page number
 * @param pageSize - The page size used for the list query
 */
export function removeServiceAccountFromListCache(
    client: ApolloClient<any>,
    serviceAccountUrn: string,
    page: number,
    pageSize: number,
): void {
    const currData: ListServiceAccountsQuery | null = client.readQuery({
        query: ListServiceAccountsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
    });

    if (currData === null) {
        return;
    }

    const existingServiceAccounts = currData?.listServiceAccounts?.serviceAccounts || [];
    const newServiceAccounts = existingServiceAccounts.filter((sa) => sa.urn !== serviceAccountUrn);
    const didRemove = existingServiceAccounts.length !== newServiceAccounts.length;

    client.writeQuery({
        query: ListServiceAccountsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
            },
        },
        data: {
            listServiceAccounts: {
                __typename: 'ListServiceAccountsResult',
                start: currData?.listServiceAccounts?.start || 0,
                count: didRemove
                    ? (currData?.listServiceAccounts?.count || 1) - 1
                    : currData?.listServiceAccounts?.count || 0,
                total: didRemove
                    ? (currData?.listServiceAccounts?.total || 1) - 1
                    : currData?.listServiceAccounts?.total || 0,
                serviceAccounts: newServiceAccounts,
            },
        },
    });
}
