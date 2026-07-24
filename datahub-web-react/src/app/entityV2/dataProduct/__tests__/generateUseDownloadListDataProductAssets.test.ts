import { act, renderHook } from '@testing-library/react-hooks';

import generateUseDownloadListDataProductAssets from '@app/entityV2/dataProduct/generateUseDownloadListDataProductAssets';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';

vi.mock('@graphql/search.generated', () => ({
    useListDataProductAssetsQuery: vi.fn(),
}));

describe('generateUseDownloadListDataProductAssets', () => {
    const refetch = vi.fn();
    const downloadInput = {
        types: [],
        query: '*',
        count: 100,
        orFilters: [],
        scrollId: null,
    };

    beforeEach(() => {
        (useListDataProductAssetsQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: {
                listDataProductAssets: {
                    count: 100,
                    total: 150,
                    searchResults: [],
                },
            },
            loading: false,
            error: undefined,
            refetch,
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('downloads data product assets with a product-scoped paginated query', async () => {
        refetch.mockResolvedValue({
            data: {
                listDataProductAssets: {
                    count: 50,
                    total: 150,
                    searchResults: [],
                },
            },
        });

        const useDownloadSearchResults = generateUseDownloadListDataProductAssets({
            urn: 'urn:li:dataProduct:analytics',
        });
        const { result } = renderHook(() =>
            useDownloadSearchResults({ variables: { input: downloadInput }, skip: true }),
        );

        expect(useListDataProductAssetsQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    urn: 'urn:li:dataProduct:analytics',
                    input: expect.objectContaining({ start: 0 }),
                },
            }),
        );
        expect(result.current.searchResults?.nextScrollId).toBe('100');

        await act(async () => {
            const nextPage = await result.current.refetch({ ...downloadInput, scrollId: '100' });
            expect(nextPage?.nextScrollId).toBeUndefined();
        });

        expect(refetch).toHaveBeenCalledWith({
            urn: 'urn:li:dataProduct:analytics',
            input: expect.objectContaining({ start: 100 }),
        });
    });
});
