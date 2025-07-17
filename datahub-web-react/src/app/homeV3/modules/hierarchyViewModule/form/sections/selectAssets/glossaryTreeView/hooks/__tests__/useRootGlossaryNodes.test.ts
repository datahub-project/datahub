import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodes';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';

// Mock the GraphQL query hook
vi.mock('@graphql/glossary.generated', () => ({
    useGetRootGlossaryNodesQuery: vi.fn(),
}));

describe('useRootGlossaryNodes', () => {
    const mockNodes = [
        { urn: 'node1', name: 'Node 1' },
        { urn: 'node2', name: 'Node 2' },
    ];

    const mockData = {
        getRootGlossaryNodes: {
            nodes: mockNodes,
            __typename: 'GlossaryNodes',
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('returns loading state when query is in progress', () => {
        // Mock the query hook to return loading state
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: true,
            data: undefined,
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: undefined,
            glossaryNodes: undefined,
            loading: true,
        });
    });

    it('returns undefined glossaryNodes when data is undefined', () => {
        // Mock the query hook to return loaded state without data
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: undefined,
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: undefined,
            glossaryNodes: undefined,
            loading: false,
        });
    });

    it('returns empty glossaryNodes array when no nodes exist', () => {
        // Mock the query hook to return data with no nodes
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryNodes: {
                    nodes: [],
                    __typename: 'GlossaryNodes',
                },
            },
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: expect.any(Object),
            glossaryNodes: [],
            loading: false,
        });
    });

    it('returns glossaryNodes when data is available', () => {
        // Mock the query hook to return data with nodes
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: mockData,
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: mockData,
            glossaryNodes: mockNodes,
            loading: false,
        });
    });

    it('handles missing getRootGlossaryNodes field', () => {
        // Mock the query hook to return data without getRootGlossaryNodes
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: {},
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: {},
            glossaryNodes: [],
            loading: false,
        });
    });

    it('handles missing nodes field', () => {
        // Mock the query hook to return data without nodes
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryNodes: {
                    __typename: 'GlossaryNodes',
                },
            },
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: expect.any(Object),
            glossaryNodes: [],
            loading: false,
        });
    });

    it('handles error state gracefully', () => {
        // Mock the query hook to return error state
        vi.mocked(useGetRootGlossaryNodesQuery).mockReturnValue({
            loading: false,
            data: undefined,
            error: new Error('Test error'),
        } as any);

        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current).toEqual({
            data: undefined,
            glossaryNodes: undefined,
            loading: false,
        });
    });
});
