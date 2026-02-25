import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useRemoveOwners } from '@app/sharedV2/owners/useRemoveOwners';

import { useBatchRemoveOwnersMutation } from '@graphql/mutations.generated';

// Mock the GraphQL mutation hook
vi.mock('@graphql/mutations.generated', () => ({
    useBatchRemoveOwnersMutation: vi.fn(),
}));

describe('useRemoveOwners', () => {
    const mockOwner1: any = {
        owner: {
            urn: 'urn:li:corpuser:test1',
        },
    };

    const mockOwner2: any = {
        owner: {
            urn: 'urn:li:corpuser:test2',
        },
    };

    const mockMutation = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        (useBatchRemoveOwnersMutation as Mock).mockReturnValue([mockMutation]);
    });

    it('should be defined', () => {
        const { result } = renderHook(() => useRemoveOwners());
        expect(result.current).toBeDefined();
    });

    it('should call mutation with correct parameters when removing owners', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current([mockOwner1], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    ownerUrns: ['urn:li:corpuser:test1'],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });

    it('should handle multiple owners for removal', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current([mockOwner1, mockOwner2], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    ownerUrns: ['urn:li:corpuser:test1', 'urn:li:corpuser:test2'],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });

    it('should not call mutation when no owners are provided', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current([], 'urn:li:dataset:test');
        });

        expect(mockMutation).not.toHaveBeenCalled();
    });

    it('should not call mutation when undefined owners are provided', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current(undefined, 'urn:li:dataset:test');
        });

        expect(mockMutation).not.toHaveBeenCalled();
    });

    it('should handle empty array of owners', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current([], 'urn:li:dataset:test');
        });

        expect(mockMutation).not.toHaveBeenCalled();
    });

    it('should handle null owners gracefully', () => {
        const { result } = renderHook(() => useRemoveOwners());

        act(() => {
            result.current(null as any, 'urn:li:dataset:test');
        });

        // Should not throw an error and should not call the mutation
        expect(mockMutation).not.toHaveBeenCalled();
    });
});
