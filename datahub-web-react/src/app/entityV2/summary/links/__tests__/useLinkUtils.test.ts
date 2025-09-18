import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';

import { EntityType } from '@types';

// Mock external modules
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
    useMutationUrn: vi.fn(),
    useRefetch: vi.fn(),
}));

const removeLinkMutationMock = vi.fn();
const addLinkMutationMock = vi.fn();
const updateLinkMutationMock = vi.fn();
vi.mock('@graphql/mutations.generated', () => ({
    useRemoveLinkMutation: () => [removeLinkMutationMock],
    useAddLinkMutation: () => [addLinkMutationMock],
    useUpdateLinkMutation: () => [updateLinkMutationMock],
}));

vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
        destroy: vi.fn(),
    },
}));

vi.mock('@app/analytics', () => ({
    __esModule: true,
    default: { event: vi.fn() },
    EventType: { EntityActionEvent: 'EntityActionEvent' },
    EntityActionType: { UpdateLinks: 'UpdateLinks' },
}));

// Helpers
const mockUserContext = (user: any) => {
    (useUserContext as Mock).mockReturnValue(user);
};
const mockEntityContext = ({
    urn = 'test-urn',
    entityType = 'dataset',
    refetch = vi.fn(),
    mutationUrn = 'mutation-urn',
} = {}) => {
    (useEntityData as Mock).mockReturnValue({ urn, entityType });
    (useRefetch as Mock).mockReturnValue(refetch);
    (useMutationUrn as Mock).mockReturnValue(mutationUrn);
    return { urn, entityType, refetch, mutationUrn };
};
const baseLink = {
    url: 'http://test.com',
    associatedUrn: 'urn:foo',
    actor: { urn: 'urn:actor', type: EntityType.CorpUser, username: 'actor' },
    author: { urn: 'urn:author', type: EntityType.CorpUser, username: 'author' },
    created: { time: Date.now() },
    description: 'desc',
    label: 'label',
};

beforeEach(() => {
    vi.clearAllMocks();
});

describe('useLinkUtils', () => {
    describe('handleDeleteLink', () => {
        it('should remove a link and show success message when mutation succeeds', async () => {
            const { refetch } = mockEntityContext();
            mockUserContext({ urn: 'user-1' });
            removeLinkMutationMock.mockResolvedValueOnce({});
            const { result } = renderHook(() => useLinkUtils(baseLink));
            await act(async () => {
                await result.current.handleDeleteLink();
            });
            expect(removeLinkMutationMock).toHaveBeenCalledWith({
                variables: {
                    input: {
                        linkUrl: baseLink.url,
                        label: baseLink.label,
                        resourceUrn: baseLink.associatedUrn,
                    },
                },
            });
            expect(message.success).toHaveBeenCalledWith({ content: 'Link Removed', duration: 2 });
            expect(refetch).toHaveBeenCalled();
        });

        it('should show error message when link removal mutation fails', async () => {
            const { refetch } = mockEntityContext();
            mockUserContext({ urn: 'user-1' });
            removeLinkMutationMock.mockRejectedValueOnce(new Error('Network error'));
            const { result } = renderHook(() => useLinkUtils(baseLink));
            await act(async () => {
                await result.current.handleDeleteLink();
            });
            expect(message.destroy).toHaveBeenCalled();
            expect(message.error).toHaveBeenCalledWith({
                content: expect.stringContaining('Error removing link:'),
                duration: 2,
            });
            expect(refetch).toHaveBeenCalled();
        });

        it('should do nothing if selectedLink is null', async () => {
            mockEntityContext();
            const { result } = renderHook(() => useLinkUtils(null));
            await act(async () => {
                await result.current.handleDeleteLink();
            });
            expect(removeLinkMutationMock).not.toHaveBeenCalled();
            expect(message.success).not.toHaveBeenCalled();
        });
    });

    describe('handleAddLink', () => {
        it('should add a link, show success message, fire analytics event, and refetch when mutation succeeds', async () => {
            const { refetch, mutationUrn, entityType } = mockEntityContext();
            mockUserContext({ urn: 'user-1' });
            addLinkMutationMock.mockResolvedValueOnce({});
            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleAddLink({ url: 'http://test-add.com', label: 'Added Link' });
            });
            expect(addLinkMutationMock).toHaveBeenCalledWith({
                variables: {
                    input: {
                        linkUrl: 'http://test-add.com',
                        label: 'Added Link',
                        resourceUrn: mutationUrn,
                        settings: {
                            showInAssetPreview: false,
                        },
                    },
                },
            });
            expect(message.success).toHaveBeenCalledWith({ content: 'Link Added', duration: 2 });
            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.AddLink,
            });
            expect(refetch).toHaveBeenCalled();
        });

        it('should show error message when link add mutation fails', async () => {
            mockEntityContext();
            mockUserContext({ urn: 'user-1' });
            addLinkMutationMock.mockRejectedValueOnce(new Error('Create error'));
            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleAddLink({ url: 'bad-add-url', label: 'Bad' });
            });
            expect(message.destroy).toHaveBeenCalled();
            expect(message.error).toHaveBeenCalledWith({
                content: expect.stringContaining('Failed to add link:'),
                duration: 3,
            });
        });
    });

    describe('handleUpdateLink', () => {
        it('should update a link and show success message when mutation succeeds', async () => {
            const { refetch } = mockEntityContext();
            updateLinkMutationMock.mockResolvedValueOnce({});
            const selectedLink = { ...baseLink };
            const newData = { url: 'http://new.com', label: 'New Label' };
            const { result } = renderHook(() => useLinkUtils(selectedLink));
            await act(async () => {
                await result.current.handleUpdateLink(newData);
            });
            expect(updateLinkMutationMock).toHaveBeenCalledWith({
                variables: {
                    input: {
                        currentLabel: selectedLink.label,
                        currentUrl: selectedLink.url,
                        resourceUrn: selectedLink.associatedUrn,
                        label: 'New Label',
                        linkUrl: 'http://new.com',
                        settings: {
                            showInAssetPreview: false,
                        },
                    },
                },
            });
            expect(message.success).toHaveBeenCalledWith({ content: 'Link Updated', duration: 2 });
            expect(refetch).toHaveBeenCalled();
        });

        it('should show error message when link update mutation fails', async () => {
            const { refetch } = mockEntityContext();
            updateLinkMutationMock.mockRejectedValueOnce(new Error('Update failed'));
            const selectedLink = { ...baseLink };
            const { result } = renderHook(() => useLinkUtils(selectedLink));
            await act(async () => {
                await result.current.handleUpdateLink({ url: 'fail-url', label: 'fail-label' });
            });
            expect(message.destroy).toHaveBeenCalled();
            expect(message.error).toHaveBeenCalledWith({
                content: expect.stringContaining('Error updating link:'),
                duration: 2,
            });
            expect(refetch).toHaveBeenCalled();
        });

        it('should do nothing if selectedLink is null when handling update', async () => {
            mockEntityContext();
            const { result } = renderHook(() => useLinkUtils(null));
            await act(async () => {
                await result.current.handleUpdateLink({ url: 'some-url', label: 'some-label' });
            });
            expect(updateLinkMutationMock).not.toHaveBeenCalled();
            expect(message.success).not.toHaveBeenCalled();
        });
    });
});
