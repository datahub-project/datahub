import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';

import { PolicyMatchCondition, ResourceFilter } from '@types';

describe('useClearOnConditionChange', () => {
    const mockOnConditionChange = vi.fn();
    const testResourceFilter: ResourceFilter = {
        filter: {
            criteria: [
                {
                    field: 'URN',
                    values: [{ value: 'urn1' }, { value: 'urn2' }],
                    condition: PolicyMatchCondition.Equals,
                },
                { field: 'TAG', values: [{ value: 'tag1' }], condition: PolicyMatchCondition.Equals },
            ],
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should remove the criterion when switching from StartsWith to Equals', () => {
        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.StartsWith, 'URN', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.Equals, testResourceFilter);

        // The criterion must be removed, not kept with empty values — an empty-values
        // criterion is stored by the backend but can never match, silently making the
        // policy apply to nothing.
        const callArgs = mockOnConditionChange.mock.calls[0][1];
        expect(mockOnConditionChange).toHaveBeenCalledWith(PolicyMatchCondition.Equals, expect.anything());
        expect(callArgs.filter?.criteria?.find((c) => c.field === 'URN')).toBeUndefined();
    });

    it('should remove the criterion when switching from Equals to StartsWith', () => {
        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.Equals, 'TAG', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.StartsWith, testResourceFilter);

        const callArgs = mockOnConditionChange.mock.calls[0][1];
        expect(mockOnConditionChange).toHaveBeenCalledWith(PolicyMatchCondition.StartsWith, expect.anything());
        expect(callArgs.filter?.criteria?.find((c) => c.field === 'TAG')).toBeUndefined();
    });

    it('should not clear values when staying in same condition type', () => {
        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.Equals, 'URN', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.NotEquals, testResourceFilter);

        expect(mockOnConditionChange).toHaveBeenCalledWith(PolicyMatchCondition.NotEquals, testResourceFilter);
    });

    it('should preserve other fields when clearing specific field', () => {
        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.StartsWith, 'URN', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.Equals, testResourceFilter);

        const callArgs = mockOnConditionChange.mock.calls[0][1];
        const tagField = callArgs.filter?.criteria?.find((c) => c.field === 'TAG');

        expect(tagField?.values).toEqual([{ value: 'tag1' }]);
    });

    it('should handle empty criteria array', () => {
        const emptyFilter: ResourceFilter = { filter: { criteria: [] } };

        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.StartsWith, 'URN', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.Equals, emptyFilter);

        expect(mockOnConditionChange).toHaveBeenCalledWith(PolicyMatchCondition.Equals, emptyFilter);
    });

    it('should handle undefined filter', () => {
        const filterWithoutCriteria: ResourceFilter = {};

        const { result } = renderHook(() =>
            useClearOnConditionChange(PolicyMatchCondition.StartsWith, 'URN', mockOnConditionChange),
        );

        result.current(PolicyMatchCondition.Equals, filterWithoutCriteria);

        expect(mockOnConditionChange).toHaveBeenCalledWith(PolicyMatchCondition.Equals, filterWithoutCriteria);
    });
});
