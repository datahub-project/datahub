import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useTreeNodesFromGlossaryNodes from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodes';
import { convertGlossaryNodeToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

import { EntityType, GlossaryNode } from '@types';

// Mock the conversion function
vi.mock('../../utils', () => ({
    convertGlossaryNodeToTreeNode: vi.fn(),
}));

describe('useTreeNodesFromGlossaryNodes', () => {
    const mockNode1: GlossaryNode = {
        urn: 'node:1',
        type: EntityType.GlossaryNode,
        childrenCount: { nodesCount: 2, termsCount: 3 },
    };

    const mockNode2: GlossaryNode = {
        urn: 'node:2',
        type: EntityType.GlossaryNode,
        childrenCount: { nodesCount: 1, termsCount: 0 },
    };

    const mockTreeNode1 = {
        value: 'node:1',
        label: 'node:1',
        hasAsyncChildren: true,
        totalChildren: 5,
        entity: mockNode1,
    };

    const mockTreeNode2 = {
        value: 'node:2',
        label: 'node:2',
        hasAsyncChildren: true,
        totalChildren: 1,
        entity: mockNode2,
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(convertGlossaryNodeToTreeNode).mockReset();
    });

    it('returns empty array when glossaryNodes is undefined', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryNodes());

        expect(result.current).toEqual([]);
        expect(convertGlossaryNodeToTreeNode).not.toHaveBeenCalled();
    });

    it('returns empty array when glossaryNodes is empty array', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryNodes([]));

        expect(result.current).toEqual([]);
        expect(convertGlossaryNodeToTreeNode).not.toHaveBeenCalled();
    });

    it('converts glossary nodes to tree nodes', () => {
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValueOnce(mockTreeNode1).mockReturnValueOnce(mockTreeNode2);

        const { result } = renderHook(() => useTreeNodesFromGlossaryNodes([mockNode1, mockNode2]));

        expect(result.current).toEqual([mockTreeNode1, mockTreeNode2]);
        expect(convertGlossaryNodeToTreeNode).toHaveBeenCalledTimes(2);
    });

    it('memoizes the result based on glossaryNodes reference', () => {
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValue(mockTreeNode1);

        const initialNodes = [mockNode1];
        const { result, rerender } = renderHook(({ nodes }) => useTreeNodesFromGlossaryNodes(nodes), {
            initialProps: { nodes: initialNodes },
        });

        // First render
        const firstResult = result.current;
        expect(firstResult).toEqual([mockTreeNode1]);

        // Re-render with same nodes reference
        rerender({ nodes: initialNodes });
        expect(result.current).toBe(firstResult); // Same reference

        // Re-render with new nodes array (same content)
        rerender({ nodes: [...initialNodes] });
        expect(result.current).not.toBe(firstResult); // New reference
        expect(result.current).toEqual([mockTreeNode1]); // Same content

        // Re-render with different nodes
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValueOnce(mockTreeNode1);
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValueOnce(mockTreeNode2);
        rerender({ nodes: [mockNode1, mockNode2] });
        expect(result.current).toEqual([mockTreeNode1, mockTreeNode2]);
    });

    it('handles nodes without childrenCount', () => {
        const nodeWithoutCount: GlossaryNode = {
            ...mockNode1,
            childrenCount: undefined,
        };

        const expectedTreeNode = {
            ...mockTreeNode1,
            hasAsyncChildren: false,
            totalChildren: 0,
        };

        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValue(expectedTreeNode);

        const { result } = renderHook(() => useTreeNodesFromGlossaryNodes([nodeWithoutCount]));

        expect(result.current).toEqual([expectedTreeNode]);
    });

    it('handles nodes with zero children', () => {
        const nodeWithZeroChildren: GlossaryNode = {
            ...mockNode1,
            childrenCount: { nodesCount: 0, termsCount: 0 },
        };

        const expectedTreeNode = {
            ...mockTreeNode1,
            hasAsyncChildren: false,
            totalChildren: 0,
        };

        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValue(expectedTreeNode);

        const { result } = renderHook(() => useTreeNodesFromGlossaryNodes([nodeWithZeroChildren]));

        expect(result.current).toEqual([expectedTreeNode]);
    });
});
