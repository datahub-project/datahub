import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';

// Mock the GraphQL query hook
vi.mock('@graphql/ownership.generated', () => ({
    useListOwnershipTypesQuery: vi.fn(),
}));

describe('useOwnershipTypes', () => {
    const mockOwnershipType1 = {
        urn: 'urn:li:ownershipType:test1',
        name: 'Test Owner',
        description: 'Test Owner Description',
    };

    const mockOwnershipType2 = {
        urn: 'urn:li:ownershipType:test2',
        name: 'Test Owner 2',
        description: 'Test Owner 2 Description',
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return loading state when query is loading', () => {
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: undefined,
            loading: true,
            refetch: vi.fn(),
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.loading).toBe(true);
        expect(result.current.ownershipTypes).toEqual([]);
        expect(result.current.defaultOwnershipType).toBeUndefined();
        expect(result.current.defaultOwnershipTypeUrn).toBeUndefined();
    });

    it('should return empty array when no ownership types are available', () => {
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: {
                listOwnershipTypes: {
                    ownershipTypes: [],
                },
            },
            loading: false,
            refetch: vi.fn(),
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.loading).toBe(false);
        expect(result.current.ownershipTypes).toEqual([]);
        expect(result.current.defaultOwnershipType).toBeUndefined();
        expect(result.current.defaultOwnershipTypeUrn).toBeUndefined();
    });

    it('should return ownership types when data is available', () => {
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: {
                listOwnershipTypes: {
                    ownershipTypes: [mockOwnershipType1, mockOwnershipType2],
                },
            },
            loading: false,
            refetch: vi.fn(),
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.loading).toBe(false);
        expect(result.current.ownershipTypes).toEqual([mockOwnershipType1, mockOwnershipType2]);
        expect(result.current.defaultOwnershipType).toEqual(mockOwnershipType1);
        expect(result.current.defaultOwnershipTypeUrn).toEqual(mockOwnershipType1.urn);
    });

    it('should return the first ownership type as default when multiple types exist', () => {
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: {
                listOwnershipTypes: {
                    ownershipTypes: [mockOwnershipType2, mockOwnershipType1], // Different order
                },
            },
            loading: false,
            refetch: vi.fn(),
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.defaultOwnershipType).toEqual(mockOwnershipType2); // First in array
        expect(result.current.defaultOwnershipTypeUrn).toEqual(mockOwnershipType2.urn);
    });

    it('should handle error state properly', () => {
        const mockError = new Error('Network error');
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: undefined,
            loading: false,
            refetch: vi.fn(),
            error: mockError,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.loading).toBe(false);
        expect(result.current.error).toEqual(mockError);
        expect(result.current.ownershipTypes).toEqual([]);
        expect(result.current.defaultOwnershipType).toBeUndefined();
        expect(result.current.defaultOwnershipTypeUrn).toBeUndefined();
    });

    it('should pass empty object as default input when no input is provided', () => {
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: {
                listOwnershipTypes: {
                    ownershipTypes: [mockOwnershipType1],
                },
            },
            loading: false,
            refetch: vi.fn(),
            error: undefined,
        });

        renderHook(() => useOwnershipTypes());

        expect(useListOwnershipTypesQuery).toHaveBeenCalledWith({
            variables: {
                input: {},
            },
        });
    });

    it('should return refetch function from the query', () => {
        const mockRefetch = vi.fn();
        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: {
                listOwnershipTypes: {
                    ownershipTypes: [mockOwnershipType1],
                },
            },
            loading: false,
            refetch: mockRefetch,
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.refetch).toEqual(mockRefetch);
    });

    it('should return raw data from the query', () => {
        const mockRawData = {
            listOwnershipTypes: {
                ownershipTypes: [mockOwnershipType1],
            },
        };

        (useListOwnershipTypesQuery as Mock).mockReturnValue({
            data: mockRawData,
            loading: false,
            refetch: vi.fn(),
            error: undefined,
        });

        const { result } = renderHook(() => useOwnershipTypes());

        expect(result.current.data).toEqual(mockRawData);
    });
});
