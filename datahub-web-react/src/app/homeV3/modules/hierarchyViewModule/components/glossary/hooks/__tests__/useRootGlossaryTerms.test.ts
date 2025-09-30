import { renderHook } from '@testing-library/react-hooks';

import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryTerms';

import { useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';

vi.mock('@graphql/glossary.generated', () => ({
    useGetRootGlossaryTermsQuery: vi.fn(),
}));

describe('useRootGlossaryTerms', () => {
    const mockTerms = [
        { urn: 'urn:li:glossaryTerm:termA', name: 'Term A' },
        { urn: 'urn:li:glossaryTerm:termB', name: 'Term B' },
    ];

    beforeEach(() => {
        (useGetRootGlossaryTermsQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryTerms: {
                    terms: mockTerms,
                },
            },
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return glossary terms and loading state', () => {
        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryTerms).toEqual(mockTerms);
        expect(result.current.data?.getRootGlossaryTerms?.terms).toEqual(mockTerms);
    });

    it('should handle loading state correctly', () => {
        (useGetRootGlossaryTermsQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
        });

        const { result } = renderHook(() => useRootGlossaryTerms());
        expect(result.current.loading).toBe(true);
        expect(result.current.glossaryTerms).toBeUndefined();
    });

    it('should return empty array if no data is available', () => {
        (useGetRootGlossaryTermsQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryTerms: {
                    terms: [],
                },
            },
        });

        const { result } = renderHook(() => useRootGlossaryTerms());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryTerms).toEqual([]);
    });

    it('should return empty array if getRootGlossaryTerms is null', () => {
        (useGetRootGlossaryTermsQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryTerms: null,
            },
        });

        const { result } = renderHook(() => useRootGlossaryTerms());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryTerms).toEqual([]);
    });

    it('should return empty array if terms are null', () => {
        (useGetRootGlossaryTermsQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryTerms: {
                    terms: null,
                },
            },
        });

        const { result } = renderHook(() => useRootGlossaryTerms());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryTerms).toEqual([]);
    });

    it('should use cache-and-network fetch policy by default', () => {
        renderHook(() => useRootGlossaryTerms());

        expect(useGetRootGlossaryTermsQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
            }),
        );
    });
});
