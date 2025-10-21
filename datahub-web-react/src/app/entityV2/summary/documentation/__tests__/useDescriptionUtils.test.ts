import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { useDescriptionUtils } from '@app/entityV2/summary/documentation/useDescriptionUtils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { sanitizeRichText } from '@src/alchemy-components/components/Editor/utils';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '@graphql/proposals.generated';

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
vi.mock('@src/alchemy-components/components/Editor/utils', () => ({
    sanitizeRichText: vi.fn((text) => text),
}));
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));
vi.mock('@graphql/mutations.generated', () => ({
    useUpdateDescriptionMutation: vi.fn(),
}));
vi.mock('@graphql/proposals.generated', () => ({
    useProposeUpdateDescriptionMutation: vi.fn(),
}));
vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
        destroy: vi.fn(),
    },
}));

describe('useDescriptionUtils', () => {
    const urn = 'urn:li:entity:123';
    const entityType = 'sampleType';
    const description = 'Initial description';
    const entityName = 'sample entity';

    const refetchMock = vi.fn();
    const updateEntityMock = vi.fn(() => Promise.resolve());
    const updateDescriptionMutationMock = vi.fn(() => Promise.resolve());
    const proposeUpdateDescriptionMutationMock = vi.fn(() => Promise.resolve());

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
        (useProposeUpdateDescriptionMutation as Mock).mockReturnValue([proposeUpdateDescriptionMutationMock]);

        vi.useFakeTimers();
    });

    it('should initialize hook state correctly', () => {
        const { result } = renderHook(() => useDescriptionUtils());

        expect(result.current.updatedDescription).toBe(description);
        expect(result.current.emptyDescriptionText).toBe(`Write a description for this ${entityName.toLowerCase()}`);
    });

    it('should reset descriptions when displayedDescription changes', () => {
        const { result, rerender } = renderHook(() => useDescriptionUtils());

        const newDescription = 'New initial description';
        (getAssetDescriptionDetails as Mock).mockReturnValue({
            displayedDescription: newDescription,
        });

        rerender();

        expect(result.current.updatedDescription).toBe(newDescription);
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
    });

    it('should return correct emptyDescriptionText placeholder', () => {
        (useEntityRegistryV2 as Mock).mockReturnValue({
            getEntityName: () => 'Document',
        });

        const { result } = renderHook(() => useDescriptionUtils());

        expect(result.current.emptyDescriptionText).toBe('Write a description for this document');
    });

    it('should initialize showProposalNote state as false', () => {
        const { result } = renderHook(() => useDescriptionUtils());

        expect(result.current.showProposalNote).toBe(false);
    });

    it('should update showProposalNote state when setShowProposalNote is called', () => {
        const { result } = renderHook(() => useDescriptionUtils());

        act(() => {
            result.current.setShowProposalNote(true);
        });

        expect(result.current.showProposalNote).toBe(true);

        act(() => {
            result.current.setShowProposalNote(false);
        });

        expect(result.current.showProposalNote).toBe(false);
    });

    it('should call proposeUpdateDescription with correct parameters when proposeDescription is called', async () => {
        const { result } = renderHook(() => useDescriptionUtils());
        const proposalNote = 'Test proposal note';

        act(() => {
            result.current.setUpdatedDescription('Updated description');
        });

        await act(async () => {
            result.current.proposeDescription(proposalNote);
        });

        expect(proposeUpdateDescriptionMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    description: 'Updated description',
                    resourceUrn: urn,
                    proposalNote,
                },
            },
        });
    });

    it('should call proposeUpdateDescription without proposalNote when none provided', async () => {
        const { result } = renderHook(() => useDescriptionUtils());

        act(() => {
            result.current.setUpdatedDescription('Updated description');
        });

        await act(async () => {
            result.current.proposeDescription();
        });

        expect(proposeUpdateDescriptionMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    description: 'Updated description',
                    resourceUrn: urn,
                    proposalNote: undefined,
                },
            },
        });
    });

    it('should show success message when proposeDescription succeeds', async () => {
        const { message } = await import('antd');
        const { result } = renderHook(() => useDescriptionUtils());

        await act(async () => {
            result.current.proposeDescription('Test note');
        });

        expect(message.success).toHaveBeenCalledWith({
            content: 'Proposed description update!',
            duration: 2,
        });
    });

    it('should show error message when proposeDescription fails', async () => {
        const { message } = await import('antd');
        const errorMessage = 'Network error';
        proposeUpdateDescriptionMutationMock.mockRejectedValueOnce(new Error(errorMessage));

        const { result } = renderHook(() => useDescriptionUtils());

        await act(async () => {
            result.current.proposeDescription('Test note');
        });

        expect(message.destroy).toHaveBeenCalled();
        expect(message.error).toHaveBeenCalledWith({
            content: `Failed to propose: \n ${errorMessage}`,
            duration: 3,
        });
    });

    it('should call sanitizeRichText when proposing description', async () => {
        const { result } = renderHook(() => useDescriptionUtils());
        const testDescription = '<p>Test description with <b>HTML</b></p>';

        act(() => {
            result.current.setUpdatedDescription(testDescription);
        });

        await act(async () => {
            result.current.proposeDescription('Test note');
        });

        expect(sanitizeRichText).toHaveBeenCalledWith(testDescription);
    });

    it('should call sanitizeRichText when updating description via new mutation path', async () => {
        (useEntityUpdate as Mock).mockReturnValue(undefined);
        const { result } = renderHook(() => useDescriptionUtils());
        const testDescription = '<p>Test description with <b>HTML</b></p>';

        act(() => {
            result.current.setUpdatedDescription(testDescription);
        });

        await act(async () => {
            result.current.handleDescriptionUpdate();
        });

        expect(sanitizeRichText).toHaveBeenCalledWith(testDescription);
    });
});
