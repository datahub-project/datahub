import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRootGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodes';
import useRootGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodesAndTerms';
import useRootGlossaryTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryTerms';

// Mock the dependent hooks
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodes',
);
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryTerms',
);

describe('useRootGlossaryNodesAndTerms', () => {
    const mockNodes = [
        { urn: 'node1', name: 'Node 1' },
        { urn: 'node2', name: 'Node 2' },
    ];

    const mockTerms = [
        { urn: 'term1', name: 'Term 1' },
        { urn: 'term2', name: 'Term 2' },
    ];

    beforeEach(() => {
        vi.clearAllMocks();

        // Reset mock implementations
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: undefined } as any);

        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: undefined } as any);
    });

    it('returns empty arrays when both nodes and terms are undefined', () => {
        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: [],
            glossaryNodes: undefined,
            glossaryTerms: undefined,
        });
    });

    it('returns only nodes when terms are undefined', () => {
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: mockNodes } as any);

        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: mockNodes,
            glossaryNodes: mockNodes,
            glossaryTerms: undefined,
        });
    });

    it('returns only terms when nodes are undefined', () => {
        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: mockTerms } as any);

        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: mockTerms,
            glossaryNodes: undefined,
            glossaryTerms: mockTerms,
        });
    });

    it('combines nodes and terms correctly', () => {
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: mockNodes } as any);

        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: mockTerms } as any);

        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: [...mockNodes, ...mockTerms],
            glossaryNodes: mockNodes,
            glossaryTerms: mockTerms,
        });
    });

    it('handles empty arrays from hooks', () => {
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: [] } as any);

        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: [] } as any);

        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: [],
            glossaryNodes: [],
            glossaryTerms: [],
        });
    });

    it('memoizes glossaryItems properly', () => {
        // First render - no data
        const { result, rerender } = renderHook(() => useRootGlossaryNodesAndTerms());
        const firstResult = result.current;

        // Second render - same data (undefined)
        rerender();
        expect(result.current.glossaryItems).toBe(firstResult.glossaryItems);

        // Third render - add nodes
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: mockNodes } as any);
        rerender();

        // Should be new reference
        expect(result.current.glossaryItems).not.toBe(firstResult.glossaryItems);
        expect(result.current.glossaryItems).toEqual(mockNodes);

        // Save current reference
        const secondResult = result.current;

        // Fourth render - same data
        rerender();
        expect(result.current.glossaryItems).toBe(secondResult.glossaryItems);

        // Fifth render - add terms
        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: mockTerms } as any);
        rerender();

        // Should be new reference
        expect(result.current.glossaryItems).not.toBe(secondResult.glossaryItems);
        expect(result.current.glossaryItems).toEqual([...mockNodes, ...mockTerms]);

        // Save current reference
        const thirdResult = result.current;

        // Sixth render - same data
        rerender();
        expect(result.current.glossaryItems).toBe(thirdResult.glossaryItems);
    });

    it('handles partial empty arrays', () => {
        vi.mocked(useRootGlossaryNodes).mockReturnValue({ glossaryNodes: [] } as any);

        vi.mocked(useRootGlossaryTerms).mockReturnValue({ glossaryTerms: mockTerms } as any);

        const { result } = renderHook(() => useRootGlossaryNodesAndTerms());

        expect(result.current).toEqual({
            glossaryItems: mockTerms,
            glossaryNodes: [],
            glossaryTerms: mockTerms,
        });
    });
});
