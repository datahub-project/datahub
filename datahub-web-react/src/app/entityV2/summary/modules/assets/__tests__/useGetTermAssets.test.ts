import { act, renderHook } from '@testing-library/react-hooks';
import { useHistory } from 'react-router';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetTermAssets } from '@app/entityV2/summary/modules/assets/useGetTermAssets';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));
vi.mock('react-router', () => ({
    useHistory: vi.fn(),
}));

describe('useGetTermAssets', () => {
    const urn = 'urn:li:glossaryTerm:term1';
    const entityType = EntityType.GlossaryTerm;
    const mockHistory = { push: vi.fn() };
    const mockRegistry = {
        getGenericEntityProperties: vi.fn().mockImplementation((type, entity) => ({ ...entity, type })),
        getEntityUrl: vi.fn(),
    };

    beforeEach(() => {
        mockRegistry.getEntityUrl.mockReturnValue('/entity/url');
        (useEntityData as unknown as any).mockReturnValue({ urn, entityType });
        (useHistory as unknown as any).mockReturnValue(mockHistory);
        (useEntityRegistryV2 as unknown as any).mockReturnValue(mockRegistry);
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [
                        { entity: { urn: 'urn:li:glossaryTerm:term1', type: 'GlossaryTerm' } },
                        { entity: { urn: 'urn:li:glossaryTerm:term2', type: 'GlossaryTerm' } },
                    ],
                    total: 2,
                },
            },
            error: undefined,
            refetch: vi.fn().mockResolvedValue({
                data: {
                    searchAcrossEntities: {
                        searchResults: [{ entity: { urn: 'urn:li:glossaryTerm:term3', type: 'GlossaryTerm' } }],
                    },
                },
            }),
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    const setup = () => renderHook(() => useGetTermAssets());

    it('should return entities, originEntities, total and not loading', () => {
        const { result } = setup();
        expect(result.current.loading).toBe(false);
        expect(result.current.total).toBe(2);
        expect(result.current.originEntities.length).toBe(2);
        expect(result.current.entities[0]?.urn).toBe('urn:li:glossaryTerm:term1');
        expect(result.current.entities[1]?.urn).toBe('urn:li:glossaryTerm:term2');
    });

    it('should return loading true if searchLoading is true', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
            error: undefined,
            refetch: vi.fn(),
        });
        const { result } = setup();
        expect(result.current.loading).toBe(true);
    });

    it('should return loading true when no data yet', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
            error: undefined,
            refetch: vi.fn(),
        });
        const { result } = setup();
        expect(result.current.loading).toBe(true);
    });

    it('should return error if there is an error', () => {
        const mockError = new Error('Test error');
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: undefined,
            error: mockError,
            refetch: vi.fn(),
        });
        const { result } = setup();
        expect(result.current.error).toBe(mockError);
    });

    it('should fetch paginated assets for non-zero start', async () => {
        const refetchMock = vi.fn().mockResolvedValue({
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity: { urn: 'urn:li:glossaryTerm:term3', type: 'GlossaryTerm' } }],
                },
            },
        });
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [{ entity: { urn: 'urn:li:glossaryTerm:term1', type: 'GlossaryTerm' } }],
                    total: 1,
                },
            },
            error: undefined,
            refetch: refetchMock,
        });

        const { result } = setup();
        let assets;
        await act(async () => {
            assets = await result.current.fetchAssets(10, 5);
        });
        expect(assets.length).toBe(1);
        expect(assets[0].urn).toBe('urn:li:glossaryTerm:term3');
        expect(refetchMock).toHaveBeenCalled();
    });

    it('should return origin entities when paginating from start=0', async () => {
        const { result } = setup();
        let assets;
        await act(async () => {
            assets = await result.current.fetchAssets(0, 10);
        });
        expect(assets.length).toBe(2);
        expect(assets[0].urn).toBe('urn:li:glossaryTerm:term1');
    });

    it('should navigate to Assets Tab with correct URL', () => {
        const { result } = setup();
        result.current.navigateToAssetsTab();
        expect(mockHistory.push).toHaveBeenCalledWith('/entity/url/Related Assets');
        expect(mockRegistry.getEntityUrl).toHaveBeenCalledWith(entityType, urn);
    });
});
