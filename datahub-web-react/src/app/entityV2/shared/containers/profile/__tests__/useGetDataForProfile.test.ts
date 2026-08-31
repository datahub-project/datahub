import { QueryResult } from '@apollo/client';
import { renderHook } from '@testing-library/react-hooks';

import useGetDataForProfile from '@app/entityV2/shared/containers/profile/useGetDataForProfile';

import { EntityType } from '@types';

const mockUseAppConfig = vi.hoisted(() => vi.fn());
const mockUseIsSeparateSiblingsMode = vi.hoisted(() => vi.fn());
const mockUseReloadableContext = vi.hoisted(() => vi.fn());
const mockUseEntityQuery = vi.hoisted(() => vi.fn());

vi.mock('@src/app/useAppConfig', () => ({
    useAppConfig: mockUseAppConfig,
}));

vi.mock('@app/entityV2/shared/useIsSeparateSiblingsMode', () => ({
    useIsSeparateSiblingsMode: mockUseIsSeparateSiblingsMode,
}));

vi.mock('@app/sharedV2/reloadableContext/hooks/useReloadableContext', () => ({
    useReloadableContext: mockUseReloadableContext,
}));

describe('useGetDataForProfile', () => {
    const mockShouldBypassCache = vi.fn();
    const mockClearCacheBypass = vi.fn();

    beforeEach(() => {
        mockUseAppConfig.mockReturnValue({
            config: { featureFlags: {} },
        });
        mockUseIsSeparateSiblingsMode.mockReturnValue(false);
        mockUseReloadableContext.mockReturnValue({
            shouldBypassCache: mockShouldBypassCache,
            clearCacheBypass: mockClearCacheBypass,
        });
        mockUseEntityQuery.mockReturnValue({
            loading: false,
            error: undefined,
            data: { dataset: { urn: 'urn:li:dataset:1' } },
            refetch: vi.fn(),
        } as unknown as QueryResult);
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should use cache-first fetchPolicy when bypass cache is false', () => {
        mockShouldBypassCache.mockReturnValue(false);

        renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        expect(mockUseEntityQuery).toHaveBeenCalledWith({
            variables: { urn: 'urn:li:dataset:1' },
            fetchPolicy: 'cache-first',
        });
    });

    it('should use network-only fetchPolicy when bypass cache is true', () => {
        mockShouldBypassCache.mockReturnValue(true);

        renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        expect(mockUseEntityQuery).toHaveBeenCalledWith({
            variables: { urn: 'urn:li:dataset:1' },
            fetchPolicy: 'network-only',
        });
    });

    it('should clear cache bypass after loading completes when bypass was true', () => {
        mockShouldBypassCache.mockReturnValue(true);
        mockUseEntityQuery.mockReturnValue({
            loading: false,
            error: undefined,
            data: { dataset: { urn: 'urn:li:dataset:1' } },
            refetch: vi.fn(),
        } as unknown as QueryResult);

        renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        expect(mockClearCacheBypass).toHaveBeenCalledWith('urn:li:dataset:1');
    });

    it('should not clear cache bypass when still loading', () => {
        mockShouldBypassCache.mockReturnValue(true);
        mockUseEntityQuery.mockReturnValue({
            loading: true,
            error: undefined,
            data: undefined,
            refetch: vi.fn(),
        } as unknown as QueryResult);

        renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        expect(mockClearCacheBypass).not.toHaveBeenCalled();
    });

    it('should not clear cache bypass when bypass was false', () => {
        mockShouldBypassCache.mockReturnValue(false);
        mockUseEntityQuery.mockReturnValue({
            loading: false,
            error: undefined,
            data: { dataset: { urn: 'urn:li:dataset:1' } },
            refetch: vi.fn(),
        } as unknown as QueryResult);

        renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        expect(mockClearCacheBypass).not.toHaveBeenCalled();
    });

    it('should update bypass cache state when urn changes', () => {
        // Start with urn:li:dataset:1 bypassing cache
        mockShouldBypassCache.mockImplementation((urn) => urn === 'urn:li:dataset:1');

        const { rerender } = renderHook(
            ({ urn }) =>
                useGetDataForProfile({
                    urn,
                    entityType: EntityType.Dataset,
                    useEntityQuery: mockUseEntityQuery,
                }),
            { initialProps: { urn: 'urn:li:dataset:1' } },
        );

        // First render should use network-only for urn:li:dataset:1
        expect(mockUseEntityQuery).toHaveBeenLastCalledWith({
            variables: { urn: 'urn:li:dataset:1' },
            fetchPolicy: 'network-only',
        });

        // Now change to urn:li:dataset:2 which should NOT bypass cache
        mockShouldBypassCache.mockImplementation((urn) => urn === 'urn:li:dataset:1');

        rerender({ urn: 'urn:li:dataset:2' });

        // Second render should use cache-first for urn:li:dataset:2
        expect(mockUseEntityQuery).toHaveBeenLastCalledWith({
            variables: { urn: 'urn:li:dataset:2' },
            fetchPolicy: 'cache-first',
        });
    });

    it('should not change fetchPolicy mid-session when context bypass list changes', () => {
        let bypassState = false;
        mockShouldBypassCache.mockImplementation(() => bypassState);

        const { rerender } = renderHook(() =>
            useGetDataForProfile({
                urn: 'urn:li:dataset:1',
                entityType: EntityType.Dataset,
                useEntityQuery: mockUseEntityQuery,
            }),
        );

        // First render with bypass = false
        expect(mockUseEntityQuery).toHaveBeenLastCalledWith({
            variables: { urn: 'urn:li:dataset:1' },
            fetchPolicy: 'cache-first',
        });

        // Now simulate bypass state changing in context (e.g., another mutation happened)
        bypassState = true;
        mockUseReloadableContext.mockReturnValue({
            shouldBypassCache: mockShouldBypassCache,
            clearCacheBypass: mockClearCacheBypass,
        });

        // Rerender with same urn
        rerender();

        // Should still use cache-first because we captured the state on mount
        expect(mockUseEntityQuery).toHaveBeenLastCalledWith({
            variables: { urn: 'urn:li:dataset:1' },
            fetchPolicy: 'cache-first',
        });
    });
});
