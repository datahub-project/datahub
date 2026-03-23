import {
    addOrUpdatePoliciesInList,
    getFieldCondition,
    getFieldValues,
    removeFromListPoliciesCache,
    setFieldCondition,
    setFieldValues,
    updateListPoliciesCache,
} from '@app/permissions/policy/policyUtils';

import { PolicyMatchCondition } from '@types';

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
    it('should get condition for a given field', () => {
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

    it('should fall back to alternate field (for deprecated fields)', () => {
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

    it('should prefer primary field over alternate field', () => {
        const filter = {
            criteria: [
                {
                    condition: PolicyMatchCondition.StartsWith,
                    field: 'RESOURCE_TYPE',
                    values: [{ value: 'container' }],
                },
                {
                    condition: PolicyMatchCondition.Equals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }],
                },
            ],
        };

        expect(getFieldCondition(filter, 'TYPE', 'RESOURCE_TYPE')).toBe(PolicyMatchCondition.Equals);
    });

    it('should return null when field is absent', () => {
        expect(getFieldCondition({ criteria: [] }, 'TYPE')).toBeNull();
        expect(getFieldCondition(undefined, 'TYPE')).toBeNull();
    });
});
describe('setFieldValues', () => {
    it('should remove a field if you pass in an empty array', () => {
        const filter = {
            criteria: [
                { condition: PolicyMatchCondition.Equals, field: 'RESOURCE_TYPE', values: [{ value: 'dataset' }] },
                { condition: PolicyMatchCondition.Equals, field: 'TYPE', values: [{ value: 'dataJob' }] },
            ],
        };

        expect(setFieldValues(filter, 'RESOURCE_TYPE', [])).toMatchObject({
            criteria: [{ condition: PolicyMatchCondition.Equals, field: 'TYPE', values: [{ value: 'dataJob' }] }],
        });
    });

    it('should default to Equals condition when no condition is specified', () => {
        expect(setFieldValues({ criteria: [] }, 'TYPE', [{ value: 'dataFlow' }])).toMatchObject({
            criteria: [{ condition: PolicyMatchCondition.Equals, field: 'TYPE', values: [{ value: 'dataFlow' }] }],
        });
    });

    it('should use an explicit condition when provided', () => {
        expect(
            setFieldValues({ criteria: [] }, 'TYPE', [{ value: 'dataset' }], PolicyMatchCondition.NotEquals),
        ).toMatchObject({
            criteria: [{ condition: PolicyMatchCondition.NotEquals, field: 'TYPE', values: [{ value: 'dataset' }] }],
        });
    });

    it('should preserve existing condition when updating values without specifying a condition', () => {
        const filter = {
            criteria: [{ condition: PolicyMatchCondition.NotEquals, field: 'TYPE', values: [{ value: 'dataset' }] }],
        };

        expect(setFieldValues(filter, 'TYPE', [{ value: 'dataset' }, { value: 'dataJob' }])).toMatchObject({
            criteria: [
                {
                    condition: PolicyMatchCondition.NotEquals,
                    field: 'TYPE',
                    values: [{ value: 'dataset' }, { value: 'dataJob' }],
                },
            ],
        });
    });
});

describe('setFieldCondition', () => {
    it('should update the condition of an existing criterion', () => {
        const filter = {
            criteria: [{ condition: PolicyMatchCondition.Equals, field: 'TYPE', values: [{ value: 'dataset' }] }],
        };

        expect(setFieldCondition(filter, 'TYPE', PolicyMatchCondition.NotEquals)).toMatchObject({
            criteria: [{ condition: PolicyMatchCondition.NotEquals, field: 'TYPE', values: [{ value: 'dataset' }] }],
        });
    });

    it('should create a placeholder criterion so values added later inherit the condition', () => {
        const filter = { criteria: [] };
        const updated = setFieldCondition(filter, 'TYPE', PolicyMatchCondition.NotEquals);

        // condition is stored even with empty values
        expect(getFieldCondition(updated, 'TYPE')).toBe(PolicyMatchCondition.NotEquals);

        // when values are then set without an explicit condition, they inherit NOT_EQUALS
        const withValues = setFieldValues(updated, 'TYPE', [{ value: 'dataset' }]);
        expect(withValues).toMatchObject({
            criteria: [{ condition: PolicyMatchCondition.NotEquals, field: 'TYPE', values: [{ value: 'dataset' }] }],
        });
    });

    it('should not affect other criteria', () => {
        const filter = {
            criteria: [
                { condition: PolicyMatchCondition.Equals, field: 'TYPE', values: [{ value: 'dataset' }] },
                { condition: PolicyMatchCondition.Equals, field: 'DOMAIN', values: [{ value: 'urn:li:domain:d1' }] },
            ],
        };

        const result = setFieldCondition(filter, 'TYPE', PolicyMatchCondition.NotEquals);
        const domainCriterion = result.criteria?.find((c) => c.field === 'DOMAIN');
        expect(domainCriterion?.condition).toBe(PolicyMatchCondition.Equals);
    });
});
