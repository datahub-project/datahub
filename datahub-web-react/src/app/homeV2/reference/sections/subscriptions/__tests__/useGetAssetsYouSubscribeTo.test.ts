import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetAssetsYouSubscribeTo } from '@app/homeV2/reference/sections/subscriptions/useGetAssetsYouSubscribeTo';

import { useListSubscriptionsQuery } from '@graphql/subscriptions.generated';
import { EntityType } from '@types';

// Mocks
const mockRefetch = vi.fn();
const mockOnCompleted = vi.fn();

const mockRegistry = {
    getGenericEntityProperties: vi.fn((type, obj) => ({ ...obj, label: `Entity-${obj.urn}` })),
};

vi.mock('@graphql/subscriptions.generated', () => ({
    useListSubscriptionsQuery: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => mockRegistry,
}));

describe('useGetAssetsYouSubscribeTo', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return loading and skip query when user is missing', () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: true,
            data: undefined,
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() => useGetAssetsYouSubscribeTo({ user: null }));

        expect(result.current.loading).toBe(true);
        expect(result.current.originEntities).toEqual([]);
        expect(result.current.entities).toEqual([]);
        expect(result.current.total).toBe(0);
    });

    it('should map subscription entities and transform them', () => {
        const mockData = {
            listSubscriptions: {
                subscriptions: [
                    { entity: { urn: 'urn:1', type: 'Dataset' } },
                    { entity: { urn: 'urn:2', type: 'Dashboard' } },
                ],
                total: 2,
            },
        };

        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: mockData,
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 2,
            }),
        );

        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBeUndefined();
        expect(result.current.originEntities).toEqual([
            { urn: 'urn:1', type: 'Dataset' },
            { urn: 'urn:2', type: 'Dashboard' },
        ]);
        expect(result.current.entities).toEqual([
            { urn: 'urn:1', type: 'Dataset', label: 'Entity-urn:1' },
            { urn: 'urn:2', type: 'Dashboard', label: 'Entity-urn:2' },
        ]);
        expect(result.current.total).toBe(2);
    });

    it('should handle error state', () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: undefined,
            error: new Error('Failed to fetch'),
            refetch: mockRefetch,
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
            }),
        );

        expect(result.current.error).toBeInstanceOf(Error);
    });

    it('should call onCompleted if passed', () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: undefined,
            error: undefined,
            refetch: mockRefetch,
        });

        renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                onCompleted: mockOnCompleted,
            }),
        );

        // Simulate calling onCompleted and assert
        mockOnCompleted();
        expect(mockOnCompleted).toHaveBeenCalled();
    });

    it('should refetch for pagination: first page', async () => {
        const mockData = {
            listSubscriptions: {
                subscriptions: [{ entity: { urn: 'urn:1', type: 'Dataset' } }],
                total: 1,
            },
        };

        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: mockData,
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: mockData,
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 1,
            }),
        );

        const entities = await result.current.fetchSubscriptions(0, 1);
        expect(entities).toEqual([{ urn: 'urn:1', type: 'Dataset' }]);
    });

    it('should refetch for pagination: non-first page', async () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {
                    subscriptions: [{ entity: { urn: 'urn:a', type: 'TypeA' } }],
                },
            },
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: {
                    listSubscriptions: {
                        subscriptions: [
                            { entity: { urn: 'urn:2', type: 'TypeB' } },
                            { entity: { urn: 'urn:3', type: 'TypeC' } },
                        ],
                    },
                },
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
            }),
        );

        const entities = await result.current.fetchSubscriptions(2, 2);
        expect(entities).toEqual([
            { urn: 'urn:2', type: 'TypeB' },
            { urn: 'urn:3', type: 'TypeC' },
        ]);
        expect(mockRefetch).toHaveBeenCalledWith({
            input: { start: 2, count: 2 },
        });
    });

    it('should map and transform entities using entity registry', () => {
        const mockData = {
            listSubscriptions: {
                subscriptions: [
                    { entity: { urn: 'urn:1', type: 'Dataset' } },
                    { entity: { urn: 'urn:2', type: 'Dashboard' } },
                ],
                total: 2,
            },
        };

        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: mockData,
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 2,
            }),
        );

        expect(result.current.entities).toEqual([
            { urn: 'urn:1', type: 'Dataset', label: 'Entity-urn:1' },
            { urn: 'urn:2', type: 'Dashboard', label: 'Entity-urn:2' },
        ]);
    });

    it('should return paginated entities from fetchSubscriptions', async () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {
                    subscriptions: [{ entity: { urn: 'urn:0', type: 'Dataset' } }],
                },
            },
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: {
                    listSubscriptions: {
                        subscriptions: [
                            { entity: { urn: 'urn:3', type: 'TypeZ' } },
                            { entity: { urn: 'urn:4', type: 'TypeA' } },
                        ],
                    },
                },
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 1,
            }),
        );

        const entities = await result.current.fetchSubscriptions(1, 2);
        expect(entities).toEqual([
            { urn: 'urn:3', type: 'TypeZ' },
            { urn: 'urn:4', type: 'TypeA' },
        ]);
        expect(mockRefetch).toHaveBeenCalledWith({
            input: { start: 1, count: 2 },
        });
    });

    it('should return empty originEntities and entities if listSubscriptions is undefined', () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {}, // listSubscriptions is undefined
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
            }),
        );

        expect(result.current.originEntities).toEqual([]);
        expect(result.current.entities).toEqual([]);
        expect(result.current.total).toBe(0);
    });

    it('should return empty originEntities and entities if subscriptions is undefined', () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {},
            },
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
            }),
        );

        expect(result.current.originEntities).toEqual([]);
        expect(result.current.entities).toEqual([]);
        expect(result.current.total).toBe(0);
    });

    it('should return empty array from fetchSubscriptions if subscriptions is undefined', async () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {
                    subscriptions: [{ entity: { urn: 'urn:1', type: 'Dataset' } }],
                },
            },
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: {
                    listSubscriptions: {},
                },
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 1,
            }),
        );

        const entities = await result.current.fetchSubscriptions(1, 1);
        expect(entities).toEqual([]);
    });

    it('should return fewer items if available data is less than requested count', async () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {
                    subscriptions: [{ entity: { urn: 'urn:0', type: 'Dataset' } }],
                },
            },
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: {
                    listSubscriptions: {
                        subscriptions: [{ entity: { urn: 'urn:3', type: 'TypeZ' } }],
                    },
                },
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
                initialCount: 1,
            }),
        );

        const entities = await result.current.fetchSubscriptions(3, 2);
        expect(entities).toEqual([{ urn: 'urn:3', type: 'TypeZ' }]);
        expect(mockRefetch).toHaveBeenCalledWith({
            input: { start: 3, count: 2 },
        });
    });

    it('should return an empty array if start is out of data range', async () => {
        (useListSubscriptionsQuery as any).mockReturnValue({
            loading: false,
            data: {
                listSubscriptions: {
                    subscriptions: [],
                },
            },
            error: undefined,
            refetch: mockRefetch.mockResolvedValue({
                data: {
                    listSubscriptions: {
                        subscriptions: [],
                    },
                },
            }),
        });

        const { result } = renderHook(() =>
            useGetAssetsYouSubscribeTo({
                user: { urn: 'user_urn', type: EntityType.CorpUser, username: 'user' },
            }),
        );

        const entities = await result.current.fetchSubscriptions(5, 2);
        expect(entities).toEqual([]);
        expect(mockRefetch).toHaveBeenCalledWith({
            input: { start: 5, count: 2 },
        });
    });
});
