import { renderHook } from '@testing-library/react-hooks';
import { ApolloClient, InMemoryCache } from '@apollo/client';
import { DomainMock1, DomainMock3, expectedResult } from '../../../Mocks';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    addToListDomainsCache,
    removeFromListDomainsCache,
    updateListDomainsCache,
    useSortedDomains,
    getParentDomains,
} from '../utils';
import { Entity, EntityType } from '../../../types.generated';

const apolloClient = new ApolloClient({
    cache: new InMemoryCache(),
});

describe('Domain V2 utils tests', () => {
    test('addToListDomainsCache -> should add a new domain to the cache list', async () => {
        const newDomain = 'new-domain.com';
        const pageSize = 10;

        const mockWriteQuery = vi.fn();
        ApolloClient.prototype.writeQuery = mockWriteQuery;

        addToListDomainsCache(apolloClient, newDomain, pageSize, 'example.com');
        expect(mockWriteQuery).toHaveBeenCalled();
        const args = mockWriteQuery.mock.calls;
        const expectedValue = {
            query: {
                kind: 'Document',
                definitions: expect.anything(),
                loc: expect.anything(),
            },

            variables: { input: { start: 0, count: 10, parentDomain: 'example.com' } },
            data: { listDomains: { start: 0, count: 1, total: 1, domains: ['new-domain.com'] } },
        };

        expect(args[0][0]).toMatchObject(expectedValue);
    });
    test('updateListDomainsCache -> should update and add a domain to the cache list', async () => {
        const urn = 'urn123';
        const id = 'domain123';
        const name = 'New Domain';
        const description = 'A test domain';
        const parentDomain = 'example.com';

        const initialDomains = [
            {
                id: 'domain1',
                name: 'Domain 1',
                description: 'Description for Domain 1',
                parentDomain: 'example.com',
            },
            {
                id: 'domain2',
                name: 'Domain 2',
                description: 'Description for Domain 2',
                parentDomain: 'example.com',
            },
        ];

        const mockReadQuery = vi.fn().mockReturnValue({
            listDomains: {
                start: 0,
                count: initialDomains.length,
                total: initialDomains.length,
                domains: initialDomains,
            },
        });

        const mockWriteQuery = vi.fn();
        ApolloClient.prototype.readQuery = mockReadQuery;
        ApolloClient.prototype.writeQuery = mockWriteQuery;

        updateListDomainsCache(apolloClient, urn, id, name, description, parentDomain);
        expect(mockReadQuery).toHaveBeenCalled();
        expect(mockWriteQuery).toHaveBeenCalled();
        const args = mockWriteQuery.mock.calls;

        const expectedResponseAfterUpdate = [
            {
                urn,
                id,
                type: EntityType.Domain,
                properties: {
                    name,
                    description: description || null,
                },
                ownership: null,
                entities: null,
                children: null,
                dataProducts: null,
                parentDomains: null,
                displayProperties: null,
            },
            ...initialDomains,
        ];

        expect(args[0][0]?.data?.listDomains?.domains).toMatchObject(expectedResponseAfterUpdate);
    });
    test('removeFromListDomainsCache -> should remove a domain from the cache list', async () => {
        const urn = 'urn1';
        const pageSize = 1000;
        const page = 1;

        const initialDomains = [
            {
                urn: 'urn1',
                id: 'domain1',
                name: 'Domain 1',
                description: 'Description for Domain 1',
                parentDomain: 'example.com',
            },
            {
                urn: 'urn2',
                id: 'domain2',
                name: 'Domain 2',
                description: 'Description for Domain 2',
                parentDomain: 'example.com',
            },
        ];
        const mockReadQuery = vi.fn().mockReturnValue({
            listDomains: {
                start: 0,
                count: initialDomains.length,
                total: initialDomains.length,
                domains: initialDomains,
            },
        });

        const mockWriteQuery = vi.fn();
        ApolloClient.prototype.readQuery = mockReadQuery;
        ApolloClient.prototype.writeQuery = mockWriteQuery;

        removeFromListDomainsCache(apolloClient, urn, page, pageSize);
        expect(mockReadQuery).toHaveBeenCalled();
        expect(mockWriteQuery).toHaveBeenCalled();
        const args = mockWriteQuery.mock.calls;
        const expectedResultAfterDelete = [
            {
                urn: 'urn2',
                id: 'domain2',
                name: 'Domain 2',
                description: 'Description for Domain 2',
                parentDomain: 'example.com',
            },
        ];
        expect(args[0][0]?.data?.listDomains?.domains).toMatchObject(expectedResultAfterDelete);
    });
    test('useSortedDomains -> should return all domains in an unsorted format if sortBy by is not provided', () => {
        const unsortedDomains = [DomainMock3[1], DomainMock3[0]];

        const { result } = renderHook(() => {
            const entityRegistry = useEntityRegistry();
            entityRegistry.register(DomainMock3[0]);
            entityRegistry.register(DomainMock3[1]);

            return useSortedDomains(unsortedDomains as unknown as Entity[]);
        });
        expect(result.current).toStrictEqual(unsortedDomains);
    });
    test('useSortedDomains -> should return all domains in a sorted format', () => {
        const { result } = renderHook(() => {
            return useSortedDomains(DomainMock3 as unknown as Entity[], 'displayName');
        });
        expect(result.current).toStrictEqual(DomainMock3);
    });
    test('getParentDomains -> should get all parent domains', () => {
        const { result } = renderHook(() => {
            const entityRegistry = useEntityRegistry();
            entityRegistry.register(DomainMock1);

            return getParentDomains(DomainMock3[0] as unknown as Entity, entityRegistry);
        });
        expect(result.current).toStrictEqual(expectedResult);
    });
});
