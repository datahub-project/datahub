import { renderHook } from '@testing-library/react-hooks';

import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryNodesAndTerms';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { ENTITY_INDEX_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, SortOrder } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

vi.mock('@app/entityV2/glossaryNode/utils', () => ({
    isGlossaryNode: (entity: any) => entity.type === EntityType.GlossaryNode,
}));

vi.mock('@app/entityV2/glossaryTerm/utils', () => ({
    isGlossaryTerm: (entity: any) => entity.type === EntityType.GlossaryTerm,
}));

describe('useGlossaryNodesAndTerms', () => {
    const mockGlossaryNode = {
        urn: 'urn:li:glossaryNode:node1',
        type: EntityType.GlossaryNode,
        properties: { name: 'Node A' },
    };
    const mockGlossaryTerm = {
        urn: 'urn:li:glossaryTerm:term1',
        type: EntityType.GlossaryTerm,
        properties: { name: 'Term A' },
    };
    const mockRefetch = vi.fn();

    beforeEach(() => {
        vi.useFakeTimers();
        vi.resetAllMocks(); // Reset mocks before each test
        (useGetSearchResultsForMultipleQuery as unknown as any).mockImplementation((props) => {
            if (props.skip) {
                return {
                    loading: false,
                    data: undefined, // Data should be undefined when skip is true
                    error: undefined,
                    refetch: mockRefetch,
                };
            }
            // Call onCompleted directly, as Apollo Client would
            if (props.onCompleted) {
                props.onCompleted();
            }
            return {
                loading: false,
                data: {
                    searchAcrossEntities: {
                        searchResults: [{ entity: mockGlossaryNode }, { entity: mockGlossaryTerm }],
                        total: 2,
                    },
                },
                error: undefined,
                refetch: mockRefetch,
            };
        });
    });

    afterEach(() => {
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
        vi.resetAllMocks();
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables for parentGlossaryNodeUrn', () => {
        const parentUrn = 'urn:li:glossaryNode:parent';
        renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: parentUrn, count: 10 }));

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: '*',
                        types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                        orFilters: [{ and: [{ field: 'parentNode', values: [parentUrn] }] }],
                        sortInput: {
                            sortCriteria: [
                                { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                                { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                            ],
                        },
                        searchFlags: {
                            skipCache: true,
                        },
                    },
                },
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                skip: false,
            }),
        );
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables for glossaryNodesAndTermsUrns', () => {
        const urns = ['urn:li:glossaryNode:node1', 'urn:li:glossaryTerm:term1'];
        renderHook(() => useGlossaryNodesAndTerms({ glossaryNodesAndTermsUrns: urns, count: 10 }));

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: '*',
                        types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                        orFilters: [{ and: [{ field: 'urn', values: urns }] }],
                        sortInput: {
                            sortCriteria: [
                                { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                                { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                            ],
                        },
                        searchFlags: {
                            skipCache: true,
                        },
                    },
                },
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                skip: false,
            }),
        );
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables for both parentGlossaryNodeUrn and glossaryNodesAndTermsUrns', () => {
        const parentUrn = 'urn:li:glossaryNode:parent';
        const urns = ['urn:li:glossaryNode:node1', 'urn:li:glossaryTerm:term1'];
        renderHook(() =>
            useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: parentUrn, glossaryNodesAndTermsUrns: urns, count: 10 }),
        );

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: '*',
                        types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                        orFilters: [
                            {
                                and: [
                                    { field: 'parentNode', values: [parentUrn] },
                                    { field: 'urn', values: urns },
                                ],
                            },
                        ],
                        sortInput: {
                            sortCriteria: [
                                { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                                { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                            ],
                        },
                        searchFlags: {
                            skipCache: true,
                        },
                    },
                },
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                skip: false,
            }),
        );
    });

    it('should handle loading state correctly', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ count: 10 }));
        expect(result.current.loading).toBe(true);
    });

    it('should handle no data gracefully', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [],
                    total: undefined,
                },
            },
            error: undefined,
            refetch: mockRefetch,
        });

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ count: 10 }));
        expect(result.current.entities?.length).toBe(0);
        expect(result.current.glossaryNodes?.length).toBe(0);
        expect(result.current.glossaryTerms?.length).toBe(0);
        expect(result.current.total).toBeUndefined();
    });

    it('should use provided fetchPolicy', () => {
        renderHook(() => useGlossaryNodesAndTerms({ count: 10, fetchPolicy: 'network-only' }));

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                fetchPolicy: 'network-only',
            }),
        );
    });
});
