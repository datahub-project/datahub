import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { EntityActionType, EventType } from '@app/analytics';
// Helpers
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
vi.mock('@graphql/mutations.generated', () => ({
    useRemoveLinkMutation: () => [removeLinkMutationMock],
    useAddLinkMutation: () => [addLinkMutationMock],
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

// Mock link
const mockDeleteLinkInput = {
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

            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleDeleteLink(mockDeleteLinkInput);
            });

            expect(removeLinkMutationMock).toHaveBeenCalledWith({
                variables: { input: { linkUrl: 'http://test.com', resourceUrn: 'urn:foo' } },
            });
            expect(message.success).toHaveBeenCalledWith({ content: 'Link Removed', duration: 2 });
            expect(refetch).toHaveBeenCalled();
        });

        it('should show error message when link removal fails', async () => {
            const { refetch } = mockEntityContext();
            mockUserContext({ urn: 'user-1' });

            removeLinkMutationMock.mockRejectedValueOnce(new Error('Network issue'));

            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleDeleteLink(mockDeleteLinkInput);
            });

            expect(message.destroy).toHaveBeenCalled();
            expect(message.error).toHaveBeenCalledWith({
                content: expect.stringContaining('Error removing link:'),
                duration: 2,
            });
            expect(refetch).toHaveBeenCalled();
        });
    });

    describe('handleAddLink', () => {
        it('should add a link, show success message, trigger analytics event, and refetch when mutation succeeds', async () => {
            const { refetch, mutationUrn, entityType } = mockEntityContext();
            mockUserContext({ urn: 'user-1' });

            addLinkMutationMock.mockResolvedValueOnce({});

            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleAddLink({ url: 'http://test.com.com', label: 'Test Link' });
            });

            expect(addLinkMutationMock).toHaveBeenCalledWith({
                variables: {
                    input: { linkUrl: 'http://test.com.com', label: 'Test Link', resourceUrn: mutationUrn },
                },
            });

            expect(message.success).toHaveBeenCalledWith({ content: 'Link Added', duration: 2 });

            expect(analytics.event).toHaveBeenCalledWith({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.UpdateLinks,
            });

            expect(refetch).toHaveBeenCalled();
        });

        it('should show error message when link add mutation fails', async () => {
            mockEntityContext();
            mockUserContext({ urn: 'user-1' });

            addLinkMutationMock.mockRejectedValueOnce(new Error('Add failed'));

            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleAddLink({ url: 'bad-url', label: 'Bad' });
            });

            expect(message.destroy).toHaveBeenCalled();
            expect(message.error).toHaveBeenCalledWith({
                content: expect.stringContaining('Failed to add link:'),
                duration: 3,
            });
        });

        it('should show error message when adding link without a user context', async () => {
            mockEntityContext();
            mockUserContext(undefined);

            const { result } = renderHook(() => useLinkUtils());
            await act(async () => {
                await result.current.handleAddLink({ url: 'test', label: 'no-user' });
            });

            expect(message.error).toHaveBeenCalledWith({
                content: 'Error adding link: no user',
                duration: 2,
            });
            expect(addLinkMutationMock).not.toHaveBeenCalled();
        });
    });
});
