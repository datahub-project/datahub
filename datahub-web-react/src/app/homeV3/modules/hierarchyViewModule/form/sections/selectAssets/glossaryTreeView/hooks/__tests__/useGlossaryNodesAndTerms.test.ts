import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryNodesAndTerms';

import { useScrollAcrossEntitiesQuery } from '@graphql/search.generated';
import { AndFilterInput, Entity, EntityType, GlossaryNode, GlossaryTerm, SortOrder } from '@types';

// Mock dependencies
vi.mock('@graphql/search.generated', () => ({
    useScrollAcrossEntitiesQuery: vi.fn(),
}));

// Mock constants
vi.mock('@app/searchV2/context/constants', () => ({
    ENTITY_NAME_FIELD: 'name',
}));

vi.mock('@app/searchV2/utils/constants', () => ({
    ENTITY_INDEX_FILTER_NAME: 'index',
}));

describe('useGlossaryNodesAndTerms', () => {
    const mockNode: GlossaryNode = { urn: 'node1', type: EntityType.GlossaryNode };
    const mockTerm: GlossaryTerm = {
        urn: 'term1',
        type: EntityType.GlossaryTerm,
        name: 'Term 1',
        hierarchicalName: 'Term 1',
    };
    const mockEntity: Entity = { urn: 'entity1', type: EntityType.Dataset };

    const mockData = {
        scrollAcrossEntities: {
            searchResults: [{ entity: mockNode }, { entity: mockTerm }, { entity: mockEntity }],
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();

        // Default mocks
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: false,
            data: undefined,
        } as any);
    });

    it('does not run query when no filters are provided', () => {
        renderHook(() => useGlossaryNodesAndTerms({}));

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith({
            variables: expect.any(Object),
            skip: true,
        });
    });

    it('creates parent filter when parentGlossaryNodeUrn is provided', () => {
        renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        const expectedFilters: AndFilterInput[] = [
            {
                and: [{ field: 'parentNode', values: ['parent1'] }],
            },
        ];

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith({
            variables: {
                input: expect.objectContaining({
                    orFilters: expectedFilters,
                }),
            },
            skip: false,
        });
    });

    it('creates URN filter when glossaryNodesAndTermsUrns is provided', () => {
        renderHook(() => useGlossaryNodesAndTerms({ glossaryNodesAndTermsUrns: ['urn1', 'urn2'] }));

        const expectedFilters: AndFilterInput[] = [
            {
                and: [{ field: 'urn', values: ['urn1', 'urn2'] }],
            },
        ];

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith({
            variables: {
                input: expect.objectContaining({
                    orFilters: expectedFilters,
                }),
            },
            skip: false,
        });
    });

    it('combines filters when both props are provided', () => {
        renderHook(() =>
            useGlossaryNodesAndTerms({
                parentGlossaryNodeUrn: 'parent1',
                glossaryNodesAndTermsUrns: ['urn1', 'urn2'],
            }),
        );

        const expectedFilters: AndFilterInput[] = [
            {
                and: [
                    { field: 'parentNode', values: ['parent1'] },
                    { field: 'urn', values: ['urn1', 'urn2'] },
                ],
            },
        ];

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith({
            variables: {
                input: expect.objectContaining({
                    orFilters: expectedFilters,
                }),
            },
            skip: false,
        });
    });

    it('returns correct query parameters', () => {
        renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: '*',
                    types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                    orFilters: expect.any(Array),
                    count: 1000,
                    sortInput: {
                        sortCriteria: [
                            { field: 'index', sortOrder: SortOrder.Ascending },
                            { field: 'name', sortOrder: SortOrder.Ascending },
                        ],
                    },
                },
            },
            skip: false,
        });
    });

    it('returns loading state', () => {
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: true,
            data: undefined,
        } as any);

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        expect(result.current).toEqual({
            data: undefined,
            entities: undefined,
            glossaryNodes: undefined,
            glossaryTerms: undefined,
            loading: true,
        });
    });

    it('processes entities correctly', async () => {
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: false,
            data: mockData,
        } as any);

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        await waitFor(() => {
            expect(result.current).toEqual({
                data: mockData,
                entities: [mockNode, mockTerm, mockEntity],
                glossaryNodes: [mockNode],
                glossaryTerms: [mockTerm],
                loading: false,
            });
        });
    });

    it('returns empty arrays when no entities', async () => {
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: false,
            data: { scrollAcrossEntities: { searchResults: [] } },
        } as any);

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        await waitFor(() => {
            expect(result.current).toEqual({
                data: expect.any(Object),
                entities: [],
                glossaryNodes: [],
                glossaryTerms: [],
                loading: false,
            });
        });
    });

    it('handles undefined searchResults', async () => {
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: false,
            data: { scrollAcrossEntities: null },
        } as any);

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        await waitFor(() => {
            expect(result.current).toEqual({
                data: expect.any(Object),
                entities: [],
                glossaryNodes: [],
                glossaryTerms: [],
                loading: false,
            });
        });
    });

    it('handles error state', async () => {
        vi.mocked(useScrollAcrossEntitiesQuery).mockReturnValue({
            loading: false,
            data: undefined,
            error: new Error('Test error'),
        } as any);

        const { result } = renderHook(() => useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: 'parent1' }));

        await waitFor(() => {
            expect(result.current).toEqual({
                data: undefined,
                entities: undefined,
                glossaryNodes: undefined,
                glossaryTerms: undefined,
                loading: false,
            });
        });
    });
});
