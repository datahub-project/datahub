import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { useDescriptionUtils } from '@app/entityV2/summary/documentation/useDescriptionUtils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';

// Mocks for the hooks and functions
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
    useMutationUrn: vi.fn(),
    useRefetch: vi.fn(),
    useEntityUpdate: vi.fn(),
}));
vi.mock('@app/entityV2/shared/tabs/Documentation/utils', () => ({
    getAssetDescriptionDetails: vi.fn(),
}));
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));
vi.mock('@graphql/mutations.generated', () => ({
    useUpdateDescriptionMutation: vi.fn(),
}));

describe('useDescriptionUtils', () => {
    const urn = 'urn:li:entity:123';
    const entityType = 'sampleType';
    const description = 'Initial description';
    const entityName = 'sample entity';

    const refetchMock = vi.fn();
    const updateEntityMock = vi.fn(() => Promise.resolve());
    const updateDescriptionMutationMock = vi.fn(() => Promise.resolve());

    beforeEach(() => {
        vi.clearAllMocks();

        (useEntityData as Mock).mockReturnValue({
            entityData: { description },
            urn,
            entityType,
        });
        (useMutationUrn as Mock).mockReturnValue(urn);
        (useRefetch as Mock).mockReturnValue(refetchMock);
        (useEntityUpdate as Mock).mockReturnValue(updateEntityMock);
        (getAssetDescriptionDetails as Mock).mockReturnValue({
            displayedDescription: description,
        });
        (useEntityRegistryV2 as Mock).mockReturnValue({
            getEntityName: () => entityName,
        });
        (useUpdateDescriptionMutation as Mock).mockReturnValue([updateDescriptionMutationMock]);

        vi.useFakeTimers();
    });

    it('should initialize hook state correctly', () => {
        const { result } = renderHook(() => useDescriptionUtils());

        expect(result.current.initialDescription).toBe(description);
        expect(result.current.updatedDescription).toBe(description);
        expect(result.current.isEditing).toBe(false);
        expect(result.current.emptyDescriptionText).toBe(`Write a description for this ${entityName.toLowerCase()}`);
    });

    it('should reset descriptions when displayedDescription changes', () => {
        const { result, rerender } = renderHook(() => useDescriptionUtils());

        const newDescription = 'New initial description';
        (getAssetDescriptionDetails as Mock).mockReturnValue({
            displayedDescription: newDescription,
        });

        rerender();

        expect(result.current.initialDescription).toBe(newDescription);
        expect(result.current.updatedDescription).toBe(newDescription);
    });

    it('should reset isEditing to false when urn changes', () => {
        const { result, rerender } = renderHook(() => useDescriptionUtils());

        act(() => {
            result.current.setIsEditing(true);
        });
        expect(result.current.isEditing).toBe(true);

        (useEntityData as Mock).mockReturnValue({
            entityData: { description },
            urn: 'urn:li:entity:456',
            entityType,
        });

        rerender();

        expect(result.current.isEditing).toBe(false);
    });

    it('should reset updatedDescription and disable editing on handleCancel', () => {
        const { result } = renderHook(() => useDescriptionUtils());

        act(() => {
            result.current.setUpdatedDescription('Some edited text');
            result.current.setIsEditing(true);
        });

        act(() => {
            result.current.handleCancel();
        });

        expect(result.current.updatedDescription).toBe(result.current.initialDescription);
        expect(result.current.isEditing).toBe(false);
    });

    it('should call legacy update method when updateEntity exists on handleDescriptionUpdate', async () => {
        const { result } = renderHook(() => useDescriptionUtils());

        await act(async () => {
            await result.current.handleDescriptionUpdate();
        });

        expect(updateEntityMock).toHaveBeenCalledWith({
            variables: {
                urn,
                input: { editableProperties: { description: result.current.updatedDescription || '' } },
            },
        });
    });

    it('should call new update method and refetch when updateEntity is undefined on handleDescriptionUpdate', async () => {
        (useEntityUpdate as Mock).mockReturnValue(undefined);
        const refetchMockDelayed = vi.fn();
        (useRefetch as Mock).mockReturnValue(refetchMockDelayed);

        const { result } = renderHook(() => useDescriptionUtils());

        act(() => {
            result.current.handleDescriptionUpdate();
        });

        await vi.advanceTimersByTimeAsync(2000);

        expect(updateDescriptionMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    description: result.current.updatedDescription,
                    resourceUrn: urn,
                },
            },
        });

        expect(refetchMockDelayed).toHaveBeenCalled();
        expect(result.current.isEditing).toBe(false);
    });

    it('should return correct emptyDescriptionText placeholder', () => {
        (useEntityRegistryV2 as Mock).mockReturnValue({
            getEntityName: () => 'Document',
        });

        const { result } = renderHook(() => useDescriptionUtils());

        expect(result.current.emptyDescriptionText).toBe('Write a description for this document');
    });
});
