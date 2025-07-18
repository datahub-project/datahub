import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryTerms';

import { useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';

// Mock the GraphQL query hook
vi.mock('@graphql/glossary.generated', () => ({
    useGetRootGlossaryTermsQuery: vi.fn(),
}));

describe('useRootGlossaryTerms', () => {
    const mockTerms = [
        { urn: 'term1', name: 'Term 1' },
        { urn: 'term2', name: 'Term 2' },
    ];

    const mockData = {
        getRootGlossaryTerms: {
            terms: mockTerms,
            __typename: 'GlossaryTerms',
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns loading state when query is in progress', () => {
        // Mock the query hook to return loading state
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: true,
            data: undefined,
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: undefined,
            glossaryTerms: undefined,
            loading: true,
        });
    });

    it('returns undefined glossaryTerms when data is undefined', () => {
        // Mock the query hook to return loaded state without data
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: false,
            data: undefined,
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: undefined,
            glossaryTerms: undefined,
            loading: false,
        });
    });

    it('returns empty glossaryTerms array when no terms exist', () => {
        // Mock the query hook to return data with no terms
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryTerms: {
                    terms: [],
                    __typename: 'GlossaryTerms',
                },
            },
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: expect.any(Object),
            glossaryTerms: [],
            loading: false,
        });
    });

    it('returns glossaryTerms when data is available', () => {
        // Mock the query hook to return data with terms
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: false,
            data: mockData,
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: mockData,
            glossaryTerms: mockTerms,
            loading: false,
        });
    });

    it('handles missing getRootGlossaryTerms field', () => {
        // Mock the query hook to return data without getRootGlossaryTerms
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: false,
            data: {},
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: {},
            glossaryTerms: [],
            loading: false,
        });
    });

    it('handles missing terms field', () => {
        // Mock the query hook to return data without terms
        vi.mocked(useGetRootGlossaryTermsQuery).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryTerms: {
                    __typename: 'GlossaryTerms',
                },
            },
        } as any);

        const { result } = renderHook(() => useRootGlossaryTerms());

        expect(result.current).toEqual({
            data: expect.any(Object),
            glossaryTerms: [],
            loading: false,
        });
    });
});
