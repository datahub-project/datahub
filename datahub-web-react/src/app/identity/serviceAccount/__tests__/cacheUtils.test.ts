import { ApolloClient, ApolloLink, InMemoryCache } from '@apollo/client';

import {
    CreatedServiceAccountData,
    DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE,
    addServiceAccountToListCache,
    removeServiceAccountFromListCache,
} from '@app/identity/serviceAccount/cacheUtils';

import { ListServiceAccountsDocument } from '@graphql/auth.generated';
import { EntityType } from '@types';

// Create a mock Apollo Client for testing
function createMockClient() {
    const cache = new InMemoryCache();
    const client = new ApolloClient({
        cache,
        link: ApolloLink.empty(),
    });
    return client;
}

describe('Service Account Cache Utils', () => {
    describe('addServiceAccountToListCache', () => {
        it('should add a new service account to an empty cache', () => {
            const client = createMockClient();

            // Setup: Initialize cache with empty list
            client.writeQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
                data: {
                    listServiceAccounts: {
                        __typename: 'ListServiceAccountsResult',
                        start: 0,
                        count: 0,
                        total: 0,
                        serviceAccounts: [],
                    },
                },
            });

            const newServiceAccount: CreatedServiceAccountData = {
                urn: 'urn:li:corpuser:service_test-uuid',
                name: 'service_test-uuid',
                displayName: 'Test Service Account',
                description: 'A test service account',
                createdBy: 'urn:li:corpuser:admin',
                createdAt: Date.now(),
            };

            // Act
            addServiceAccountToListCache(client, newServiceAccount);

            // Assert
            const result = client.readQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
            });

            expect(result?.listServiceAccounts?.total).toBe(1);
            expect(result?.listServiceAccounts?.serviceAccounts).toHaveLength(1);
            expect(result?.listServiceAccounts?.serviceAccounts?.[0]?.urn).toBe(newServiceAccount.urn);
            expect(result?.listServiceAccounts?.serviceAccounts?.[0]?.name).toBe(newServiceAccount.name);
            expect(result?.listServiceAccounts?.serviceAccounts?.[0]?.displayName).toBe(newServiceAccount.displayName);
        });

        it('should prepend a new service account to an existing list', () => {
            const client = createMockClient();

            // Setup: Initialize cache with existing service accounts
            client.writeQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
                data: {
                    listServiceAccounts: {
                        __typename: 'ListServiceAccountsResult',
                        start: 0,
                        count: 1,
                        total: 1,
                        serviceAccounts: [
                            {
                                __typename: 'ServiceAccount',
                                urn: 'urn:li:corpuser:service_existing',
                                type: EntityType.CorpUser,
                                name: 'service_existing',
                                displayName: 'Existing Service Account',
                                description: null,
                                createdBy: null,
                                createdAt: null,
                                updatedAt: null,
                                roles: {
                                    __typename: 'EntityRelationshipsResult',
                                    start: 0,
                                    count: 1,
                                    total: 0,
                                    relationships: [],
                                },
                            },
                        ],
                    },
                },
            });

            const newServiceAccount: CreatedServiceAccountData = {
                urn: 'urn:li:corpuser:service_new',
                name: 'service_new',
                displayName: 'New Service Account',
            };

            // Act
            addServiceAccountToListCache(client, newServiceAccount);

            // Assert
            const result = client.readQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
            });

            expect(result?.listServiceAccounts?.total).toBe(2);
            expect(result?.listServiceAccounts?.serviceAccounts).toHaveLength(2);
            // New one should be first
            expect(result?.listServiceAccounts?.serviceAccounts?.[0]?.urn).toBe(newServiceAccount.urn);
            // Existing one should be second
            expect(result?.listServiceAccounts?.serviceAccounts?.[1]?.urn).toBe('urn:li:corpuser:service_existing');
        });

        it('should not add if cache is empty (first load not occurred)', () => {
            const client = createMockClient();

            const newServiceAccount: CreatedServiceAccountData = {
                urn: 'urn:li:corpuser:service_new',
                name: 'service_new',
            };

            // Act - should not throw, just return early
            addServiceAccountToListCache(client, newServiceAccount);

            // Assert - cache should still be empty
            const result = client.readQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
            });

            expect(result).toBeNull();
        });
    });

    describe('removeServiceAccountFromListCache', () => {
        it('should remove a service account from the cache', () => {
            const client = createMockClient();

            // Setup: Initialize cache with service accounts
            client.writeQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
                data: {
                    listServiceAccounts: {
                        __typename: 'ListServiceAccountsResult',
                        start: 0,
                        count: 2,
                        total: 2,
                        serviceAccounts: [
                            {
                                __typename: 'ServiceAccount',
                                urn: 'urn:li:corpuser:service_first',
                                type: EntityType.CorpUser,
                                name: 'service_first',
                                displayName: 'First',
                                description: null,
                                createdBy: null,
                                createdAt: null,
                                updatedAt: null,
                                roles: {
                                    __typename: 'EntityRelationshipsResult',
                                    start: 0,
                                    count: 1,
                                    total: 0,
                                    relationships: [],
                                },
                            },
                            {
                                __typename: 'ServiceAccount',
                                urn: 'urn:li:corpuser:service_second',
                                type: EntityType.CorpUser,
                                name: 'service_second',
                                displayName: 'Second',
                                description: null,
                                createdBy: null,
                                createdAt: null,
                                updatedAt: null,
                                roles: {
                                    __typename: 'EntityRelationshipsResult',
                                    start: 0,
                                    count: 1,
                                    total: 0,
                                    relationships: [],
                                },
                            },
                        ],
                    },
                },
            });

            // Act
            removeServiceAccountFromListCache(
                client,
                'urn:li:corpuser:service_first',
                1,
                DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE,
            );

            // Assert
            const result = client.readQuery({
                query: ListServiceAccountsDocument,
                variables: { input: { start: 0, count: DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE } },
            });

            expect(result?.listServiceAccounts?.total).toBe(1);
            expect(result?.listServiceAccounts?.serviceAccounts).toHaveLength(1);
            expect(result?.listServiceAccounts?.serviceAccounts?.[0]?.urn).toBe('urn:li:corpuser:service_second');
        });

        it('should handle removing from an empty cache gracefully', () => {
            const client = createMockClient();

            // Act - should not throw
            removeServiceAccountFromListCache(
                client,
                'urn:li:corpuser:service_nonexistent',
                1,
                DEFAULT_SERVICE_ACCOUNT_LIST_PAGE_SIZE,
            );

            // Assert - no error thrown
            expect(true).toBe(true);
        });
    });
});
