import { PolicyMatchCondition } from '../../../../types.generated';
import {
    addOrUpdatePoliciesInList,
    updateListPoliciesCache,
    removeFromListPoliciesCache,
    getFieldValues,
    getFieldCondition,
    setFieldValues,
} from '../policyUtils';

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

describe('getFieldValues', () => {
    it('should get field values for a given field', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }, { value: 'dataJob' }],
                },
            ],
        };

        expect(getFieldValues(filter, 'TYPE')).toMatchObject([{ value: 'dataset' }, { value: 'dataJob' }]);
    });

    it('should get field values for a alternate field (for deprecated fields)', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'dataset' }, { value: 'dataJob' }],
                },
            ],
        };

        expect(getFieldValues(filter, 'TYPE', 'RESOURCE_TYPE')).toMatchObject([
            { value: 'dataset' },
            { value: 'dataJob' },
        ]);
    });

    it('should get field values for main field with alternative field given and has values', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'container' }, { value: 'dataFlow' }],
                },
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }, { value: 'dataJob' }],
                },
            ],
        };

        // should only return values from main field
        expect(getFieldValues(filter, 'TYPE', 'RESOURCE_TYPE')).toMatchObject([
            { value: 'dataset' },
            { value: 'dataJob' },
        ]);
    });
});

describe('getFieldCondition', () => {
    it('should get field values for a given field', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }],
                },
            ],
        };

        expect(getFieldCondition(filter, 'TYPE')).toBe(PolicyMatchCondition.Equals);
    });

    it('should get field values for a alternate field (for deprecated fields)', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'dataset' }],
                },
            ],
        };

        expect(getFieldCondition(filter, 'TYPE', 'RESOURCE_TYPE')).toBe(PolicyMatchCondition.Equals);
    });

    it('should get field values for main field with alternative field given and has values', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.StartsWith,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'container' }, { value: 'dataFlow' }],
                },
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }],
                },
            ],
        };

        // should only return values from main field
        expect(getFieldCondition(filter, 'TYPE', 'RESOURCE_TYPE')).toBe(PolicyMatchCondition.Equals);
    });
});
describe('setFieldValues', () => {
    it('should remove a field if you pass in an empty array', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'dataset' }],
                },
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataJob' }],
                },
            ],
        };

        expect(setFieldValues(filter, 'RESOURCE_TYPE', [])).toMatchObject({
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataJob' }],
                },
            ],
        });
    });

    it('should set values for a field properly', () => {
        const filter = {
            criteria: [],
        };

        expect(setFieldValues(filter, 'TYPE', [{ value: 'dataFlow' }])).toMatchObject({
            criteria: [
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataFlow' }],
                },
            ],
        });
    });
});
