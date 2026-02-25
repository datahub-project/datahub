import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useRemoveOwners } from '@app/sharedV2/owners/useRemoveOwners';
import { useUpdateOwners } from '@app/sharedV2/owners/useUpdateOwners';

// Mock the dependent hooks
vi.mock('@app/sharedV2/owners/useAddOwners', () => ({
    useAddOwners: vi.fn(),
}));

vi.mock('@app/sharedV2/owners/useRemoveOwners', () => ({
    useRemoveOwners: vi.fn(),
}));

describe('useUpdateOwners', () => {
    const mockAddOwners = vi.fn();
    const mockRemoveOwners = vi.fn();

    const mockNewOwner: any = {
        urn: 'urn:li:corpuser:new',
        type: 'CORP_USER',
    };

    const mockExistingOwner: any = {
        owner: {
            urn: 'urn:li:corpuser:existing',
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();

        (useAddOwners as Mock).mockReturnValue(mockAddOwners);
        (useRemoveOwners as Mock).mockReturnValue(mockRemoveOwners);
    });

    it('should be defined', () => {
        const { result } = renderHook(() => useUpdateOwners());
        expect(result.current).toBeDefined();
    });

    it('should call addOwners for new owners and removeOwners for removed owners', () => {
        const { result } = renderHook(() => useUpdateOwners());

        act(() => {
            result.current([mockNewOwner], [mockExistingOwner], 'urn:li:dataset:test');
        });

        // Should call addOwners with new owner and removeOwners with existing owner
        expect(mockAddOwners).toHaveBeenCalledWith([mockNewOwner], 'urn:li:dataset:test');
        expect(mockRemoveOwners).toHaveBeenCalledWith([mockExistingOwner], 'urn:li:dataset:test');
    });

    it('should only call addOwners when adding new owners', () => {
        const { result } = renderHook(() => useUpdateOwners());

        act(() => {
            result.current([mockNewOwner], [], 'urn:li:dataset:test');
        });

        expect(mockAddOwners).toHaveBeenCalledWith([mockNewOwner], 'urn:li:dataset:test');
        expect(mockRemoveOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
    });

    it('should only call removeOwners when removing existing owners', () => {
        const { result } = renderHook(() => useUpdateOwners());

        act(() => {
            result.current([], [mockExistingOwner], 'urn:li:dataset:test');
        });

        expect(mockAddOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
        expect(mockRemoveOwners).toHaveBeenCalledWith([mockExistingOwner], 'urn:li:dataset:test');
    });

    it('should not call either function when no changes are made', () => {
        const { result } = renderHook(() => useUpdateOwners());

        act(() => {
            result.current([], [], 'urn:li:dataset:test');
        });

        expect(mockAddOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
        expect(mockRemoveOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
    });

    it('should handle undefined parameters gracefully', () => {
        const { result } = renderHook(() => useUpdateOwners());

        act(() => {
            result.current(undefined, undefined, 'urn:li:dataset:test');
        });

        expect(mockAddOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
        expect(mockRemoveOwners).toHaveBeenCalledWith([], 'urn:li:dataset:test');
    });
});
