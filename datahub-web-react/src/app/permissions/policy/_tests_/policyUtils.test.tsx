import { addOrUpdatePoliciesInList, updateListPoliciesCache, removeFromListPoliciesCache } from '../policyUtils';

// Mock the Apollo Client readQuery and writeQuery methods
const mockReadQuery = vi.fn();
const mockWriteQuery = vi.fn();

vi.mock('@apollo/client', async () => ({
    ...((await vi.importActual('@apollo/client')) as any),
    useApolloClient: () => ({
        readQuery: mockReadQuery,
        writeQuery: mockWriteQuery,
    }),
}));

describe('addOrUpdatePoliciesInList', () => {
    it('should add a new policy to the list', () => {
        const existingPolicies = [{ urn: 'existing-urn' }];
        const newPolicies = { urn: 'new-urn' };

        const result = addOrUpdatePoliciesInList(existingPolicies, newPolicies);

        expect(result.length).toBe(existingPolicies.length + 1);
        expect(result).toContain(newPolicies);
    });

    it('should update an existing policy in the list', () => {
        const existingPolicies = [{ urn: 'existing-urn' }];
        const newPolicies = { urn: 'existing-urn', updatedField: 'new-value' };

        const result = addOrUpdatePoliciesInList(existingPolicies, newPolicies);

        expect(result.length).toBe(existingPolicies.length);
        expect(result).toContainEqual(newPolicies);
    });
});

describe('updateListPoliciesCache', () => {
    // Mock client.readQuery response
    const mockReadQueryResponse = {
        listPolicies: {
            start: 0,
            count: 1,
            total: 1,
            policies: [{ urn: 'existing-urn' }],
        },
    };

    beforeEach(() => {
        mockReadQuery.mockReturnValueOnce(mockReadQueryResponse);
    });

    it('should update the list policies cache with a new policy', () => {
        const mockClient = {
            readQuery: mockReadQuery,
            writeQuery: mockWriteQuery,
        };

        const policiesToAdd = [{ urn: 'new-urn' }];
        const pageSize = 10;

        updateListPoliciesCache(mockClient, policiesToAdd, pageSize);

        // Ensure writeQuery is called with the expected data
        expect(mockWriteQuery).toHaveBeenCalledWith({
            query: expect.any(Object),
            variables: { input: { start: 0, count: pageSize, query: undefined } },
            data: expect.any(Object),
        });
    });
});

describe('removeFromListPoliciesCache', () => {
    // Mock client.readQuery response
    const mockReadQueryResponse = {
        listPolicies: {
            start: 0,
            count: 1,
            total: 1,
            policies: [{ urn: 'existing-urn' }],
        },
    };

    beforeEach(() => {
        mockReadQuery.mockReturnValueOnce(mockReadQueryResponse);
    });

    it('should remove a policy from the list policies cache', () => {
        const mockClient = {
            readQuery: mockReadQuery,
            writeQuery: mockWriteQuery,
        };

        const urnToRemove = 'existing-urn';
        const pageSize = 10;

        removeFromListPoliciesCache(mockClient, urnToRemove, pageSize);

        // Ensure writeQuery is called with the expected data
        expect(mockWriteQuery).toHaveBeenCalledWith({
            query: expect.any(Object),
            variables: { input: { start: 0, count: pageSize } },
            data: expect.any(Object),
        });
    });
});
