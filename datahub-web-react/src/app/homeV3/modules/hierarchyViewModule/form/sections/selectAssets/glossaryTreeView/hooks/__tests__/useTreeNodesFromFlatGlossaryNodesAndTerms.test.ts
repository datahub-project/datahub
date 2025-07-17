import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useTreeNodesFromFlatGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromFlatGlossaryNodesAndTerms';
import {
    unwrapFlatGlossaryNodesToTreeNodes,
    unwrapFlatGlossaryTermsToTreeNodes,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

import { GlossaryNode, GlossaryTerm } from '@types';

// Mock the utility functions
vi.mock('@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils', () => ({
    unwrapFlatGlossaryNodesToTreeNodes: vi.fn(),
    unwrapFlatGlossaryTermsToTreeNodes: vi.fn(),
}));

describe('useTreeNodesFromFlatGlossaryNodesAndTerms', () => {
    const mockNode1: GlossaryNode = { urn: 'node1', name: 'Node 1' } as any;
    const mockNode2: GlossaryNode = { urn: 'node2', name: 'Node 2' } as any;
    const mockTerm1: GlossaryTerm = { urn: 'term1', name: 'Term 1' } as any;
    const mockTerm2: GlossaryTerm = { urn: 'term2', name: 'Term 2' } as any;

    const mockTreeNode1 = { value: 'node1', label: 'Node 1' } as any;
    const mockTreeNode2 = { value: 'node2', label: 'Node 2' } as any;
    const mockTreeTerm1 = { value: 'term1', label: 'Term 1' } as any;
    const mockTreeTerm2 = { value: 'term2', label: 'Term 2' } as any;

    beforeEach(() => {
        vi.clearAllMocks();

        // Reset mock implementations
        vi.mocked(unwrapFlatGlossaryNodesToTreeNodes).mockReset().mockReturnValue([]);

        vi.mocked(unwrapFlatGlossaryTermsToTreeNodes).mockReset().mockReturnValue([]);
    });

    it('returns empty arrays when both inputs are undefined', () => {
        const { result } = renderHook(() => useTreeNodesFromFlatGlossaryNodesAndTerms(undefined, undefined));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [],
            treeNodes: [],
        });
    });

    it('returns empty arrays when both inputs are empty', () => {
        const { result } = renderHook(() => useTreeNodesFromFlatGlossaryNodesAndTerms([], []));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [],
            treeNodes: [],
        });
    });

    it('processes nodes and terms correctly', () => {
        vi.mocked(unwrapFlatGlossaryNodesToTreeNodes).mockReturnValue([mockTreeNode1, mockTreeNode2]);

        vi.mocked(unwrapFlatGlossaryTermsToTreeNodes).mockReturnValue([mockTreeTerm1, mockTreeTerm2]);

        const { result } = renderHook(() =>
            useTreeNodesFromFlatGlossaryNodesAndTerms([mockNode1, mockNode2], [mockTerm1, mockTerm2]),
        );

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [mockTreeNode1, mockTreeNode2],
            glossaryTermsTreeNodes: [mockTreeTerm1, mockTreeTerm2],
            treeNodes: [mockTreeNode1, mockTreeNode2, mockTreeTerm1, mockTreeTerm2],
        });

        // Verify utility functions were called correctly
        expect(unwrapFlatGlossaryNodesToTreeNodes).toHaveBeenCalledWith([mockNode1, mockNode2]);
        expect(unwrapFlatGlossaryTermsToTreeNodes).toHaveBeenCalledWith([mockTerm1, mockTerm2]);
    });

    it('handles only nodes', () => {
        vi.mocked(unwrapFlatGlossaryNodesToTreeNodes).mockReturnValue([mockTreeNode1]);

        const { result } = renderHook(() => useTreeNodesFromFlatGlossaryNodesAndTerms([mockNode1], undefined));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [mockTreeNode1],
            glossaryTermsTreeNodes: [],
            treeNodes: [mockTreeNode1],
        });
    });

    it('handles only terms', () => {
        vi.mocked(unwrapFlatGlossaryTermsToTreeNodes).mockReturnValue([mockTreeTerm1]);

        const { result } = renderHook(() => useTreeNodesFromFlatGlossaryNodesAndTerms(undefined, [mockTerm1]));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [mockTreeTerm1],
            treeNodes: [mockTreeTerm1],
        });
    });

    it('handles empty arrays from utility functions', () => {
        vi.mocked(unwrapFlatGlossaryNodesToTreeNodes).mockReturnValue([]);

        vi.mocked(unwrapFlatGlossaryTermsToTreeNodes).mockReturnValue([]);

        const { result } = renderHook(() => useTreeNodesFromFlatGlossaryNodesAndTerms([mockNode1], [mockTerm1]));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [],
            treeNodes: [],
        });
    });
});
