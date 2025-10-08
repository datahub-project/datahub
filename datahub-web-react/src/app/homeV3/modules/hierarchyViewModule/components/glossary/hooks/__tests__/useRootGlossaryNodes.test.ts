import { renderHook } from '@testing-library/react-hooks';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useRootGlossaryNodes';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';

vi.mock('@graphql/glossary.generated', () => ({
    useGetRootGlossaryNodesQuery: vi.fn(),
}));

describe('useRootGlossaryNodes', () => {
    const mockNodes = [
        { urn: 'urn:li:glossaryNode:nodeA', name: 'Node A' },
        { urn: 'urn:li:glossaryNode:nodeB', name: 'Node B' },
    ];

    beforeEach(() => {
        (useGetRootGlossaryNodesQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                getRootGlossaryNodes: {
                    nodes: mockNodes,
                },
            },
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('should return glossary nodes and loading state', () => {
        const { result } = renderHook(() => useRootGlossaryNodes());

        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryNodes).toEqual(mockNodes);
        expect(result.current.data?.getRootGlossaryNodes?.nodes).toEqual(mockNodes);
    });

    it('should handle loading state correctly', () => {
        (useGetRootGlossaryNodesQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
        });

        const { result } = renderHook(() => useRootGlossaryNodes());
        expect(result.current.loading).toBe(true);
        expect(result.current.glossaryNodes).toBeUndefined();
    });

    it('should return empty array if no data is available', () => {
        (useGetRootGlossaryNodesQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryNodes: {
                    nodes: [],
                },
            },
        });

        const { result } = renderHook(() => useRootGlossaryNodes());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryNodes).toEqual([]);
    });

    it('should return empty array if getRootGlossaryNodes is null', () => {
        (useGetRootGlossaryNodesQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryNodes: null,
            },
        });

        const { result } = renderHook(() => useRootGlossaryNodes());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryNodes).toEqual([]);
    });

    it('should return empty array if nodes are null', () => {
        (useGetRootGlossaryNodesQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                getRootGlossaryNodes: {
                    nodes: null,
                },
            },
        });

        const { result } = renderHook(() => useRootGlossaryNodes());
        expect(result.current.loading).toBe(false);
        expect(result.current.glossaryNodes).toEqual([]);
    });

    it('should use cache-and-network fetch policy by default', () => {
        renderHook(() => useRootGlossaryNodes());

        expect(useGetRootGlossaryNodesQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
            }),
        );
    });
});
