import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';

// Mock the GraphQL mutation hook
vi.mock('@graphql/mutations.generated', () => ({
    useBatchAddOwnersMutation: vi.fn(),
}));

// Mock the useOwnershipTypes hook
vi.mock('@app/sharedV2/owners/useOwnershipTypes', () => ({
    useOwnershipTypes: vi.fn(),
}));

describe('useAddOwners', () => {
    const mockOwnershipType = {
        urn: 'urn:li:ownershipType:test',
        name: 'Test Owner',
        description: 'Test Owner Description',
    };

    const mockCorpUser: any = {
        urn: 'urn:li:corpuser:test',
        type: 'CORP_USER',
    };

    const mockCorpGroup: any = {
        urn: 'urn:li:corpgroup:test',
        type: 'CORP_GROUP',
    };

    const mockMutation = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: mockOwnershipType,
        });

        (useBatchAddOwnersMutation as Mock).mockReturnValue([mockMutation]);
    });

    it('should be defined', () => {
        const { result } = renderHook(() => useAddOwners());
        expect(result.current).toBeDefined();
    });

    it('should call mutation with correct parameters when adding owners', () => {
        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current([mockCorpUser], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpuser:test',
                            ownerEntityType: 'CORP_USER',
                            ownershipTypeUrn: 'urn:li:ownershipType:test',
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });

    it('should handle corp group owners correctly', () => {
        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current([mockCorpGroup], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpgroup:test',
                            ownerEntityType: 'CORP_GROUP',
                            ownershipTypeUrn: 'urn:li:ownershipType:test',
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });

    it('should handle multiple owners', () => {
        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current([mockCorpUser, mockCorpGroup], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpuser:test',
                            ownerEntityType: 'CORP_USER',
                            ownershipTypeUrn: 'urn:li:ownershipType:test',
                        },
                        {
                            ownerUrn: 'urn:li:corpgroup:test',
                            ownerEntityType: 'CORP_GROUP',
                            ownershipTypeUrn: 'urn:li:ownershipType:test',
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });

    it('should not call mutation when no owners are provided', () => {
        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current([], 'urn:li:dataset:test');
        });

        expect(mockMutation).not.toHaveBeenCalled();
    });

    it('should not call mutation when undefined owners are provided', () => {
        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current(undefined, 'urn:li:dataset:test');
        });

        expect(mockMutation).not.toHaveBeenCalled();
    });

    it('should use default ownership type from useOwnershipTypes hook', () => {
        const customOwnershipType = {
            urn: 'urn:li:ownershipType:custom',
            name: 'Custom Owner',
            description: 'Custom Owner Description',
        };

        (useOwnershipTypes as Mock).mockReturnValue({
            defaultOwnershipType: customOwnershipType,
        });

        const { result } = renderHook(() => useAddOwners());

        act(() => {
            result.current([mockCorpUser], 'urn:li:dataset:test');
        });

        expect(mockMutation).toHaveBeenCalledWith({
            variables: {
                input: {
                    owners: [
                        {
                            ownerUrn: 'urn:li:corpuser:test',
                            ownerEntityType: 'CORP_USER',
                            ownershipTypeUrn: 'urn:li:ownershipType:custom',
                        },
                    ],
                    resources: [{ resourceUrn: 'urn:li:dataset:test' }],
                },
            },
        });
    });
});
