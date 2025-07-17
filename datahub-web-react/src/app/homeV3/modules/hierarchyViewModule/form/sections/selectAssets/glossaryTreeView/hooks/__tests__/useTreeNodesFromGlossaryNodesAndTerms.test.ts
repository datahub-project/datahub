import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import {
    convertGlossaryNodeToTreeNode,
    convertGlossaryTermToTreeNode,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/utils';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

vi.mock('../../utils', () => ({
    convertGlossaryNodeToTreeNode: vi.fn(),
    convertGlossaryTermToTreeNode: vi.fn(),
}));

describe('useTreeNodesFromGlossaryNodesAndTerms', () => {
    const mockNode1: GlossaryNode = {
        urn: 'node:1',
        type: EntityType.GlossaryNode,
    };

    const mockNode2: GlossaryNode = {
        urn: 'node:2',
        type: EntityType.GlossaryNode,
    };

    const mockTerm1: GlossaryTerm = {
        urn: 'term:1',
        name: 'Term 1',
        hierarchicalName: 'Term 1',
        type: EntityType.GlossaryTerm,
    };

    const mockTerm2: GlossaryTerm = {
        urn: 'term:2',
        name: 'Term 2',
        hierarchicalName: 'Term 2',
        type: EntityType.GlossaryTerm,
    };

    const mockTreeNode1 = {
        value: 'node:1',
        label: 'node:1',
        entity: mockNode1,
    };

    const mockTreeNode2 = {
        value: 'node:2',
        label: 'node:2',
        entity: mockNode2,
    };

    const mockTreeTerm1 = {
        value: 'term:1',
        label: 'term:1',
        entity: mockTerm1,
    };

    const mockTreeTerm2 = {
        value: 'term:2',
        label: 'term:2',
        entity: mockTerm2,
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(convertGlossaryNodeToTreeNode).mockReset();
        vi.mocked(convertGlossaryTermToTreeNode).mockReset();
    });

    it('handles undefined inputs', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryNodesAndTerms(undefined, undefined));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [],
            treeNodes: [],
        });
        expect(convertGlossaryNodeToTreeNode).not.toHaveBeenCalled();
        expect(convertGlossaryTermToTreeNode).not.toHaveBeenCalled();
    });

    it('handles empty arrays', () => {
        const { result } = renderHook(() => useTreeNodesFromGlossaryNodesAndTerms([], []));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [],
            treeNodes: [],
        });
        expect(convertGlossaryNodeToTreeNode).not.toHaveBeenCalled();
        expect(convertGlossaryTermToTreeNode).not.toHaveBeenCalled();
    });

    it('converts nodes and terms separately', () => {
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValueOnce(mockTreeNode1).mockReturnValueOnce(mockTreeNode2);

        vi.mocked(convertGlossaryTermToTreeNode).mockReturnValueOnce(mockTreeTerm1).mockReturnValueOnce(mockTreeTerm2);

        const { result } = renderHook(() =>
            useTreeNodesFromGlossaryNodesAndTerms([mockNode1, mockNode2], [mockTerm1, mockTerm2]),
        );

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [mockTreeNode1, mockTreeNode2],
            glossaryTermsTreeNodes: [mockTreeTerm1, mockTreeTerm2],
            treeNodes: [mockTreeNode1, mockTreeNode2, mockTreeTerm1, mockTreeTerm2],
        });
    });

    it('handles only nodes', () => {
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValueOnce(mockTreeNode1).mockReturnValueOnce(mockTreeNode2);

        const { result } = renderHook(() => useTreeNodesFromGlossaryNodesAndTerms([mockNode1, mockNode2], undefined));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [mockTreeNode1, mockTreeNode2],
            glossaryTermsTreeNodes: [],
            treeNodes: [mockTreeNode1, mockTreeNode2],
        });
    });

    it('handles only terms', () => {
        vi.mocked(convertGlossaryTermToTreeNode).mockReturnValueOnce(mockTreeTerm1).mockReturnValueOnce(mockTreeTerm2);

        const { result } = renderHook(() => useTreeNodesFromGlossaryNodesAndTerms(undefined, [mockTerm1, mockTerm2]));

        expect(result.current).toEqual({
            glossaryNodesTreeNodes: [],
            glossaryTermsTreeNodes: [mockTreeTerm1, mockTreeTerm2],
            treeNodes: [mockTreeTerm1, mockTreeTerm2],
        });
    });

    it('memoizes results properly', () => {
        vi.mocked(convertGlossaryNodeToTreeNode).mockReturnValue(mockTreeNode1);
        vi.mocked(convertGlossaryTermToTreeNode).mockReturnValue(mockTreeTerm1);

        const initialProps = {
            nodes: [mockNode1] as GlossaryNode[] | undefined,
            terms: [mockTerm1] as GlossaryTerm[] | undefined,
        };

        const { result, rerender } = renderHook(
            ({ nodes, terms }) => useTreeNodesFromGlossaryNodesAndTerms(nodes, terms),
            { initialProps },
        );

        // First render
        const firstResult = result.current;

        // Re-render with same props
        rerender(initialProps);
        expect(result.current.glossaryNodesTreeNodes).toBe(firstResult.glossaryNodesTreeNodes);
        expect(result.current.glossaryTermsTreeNodes).toBe(firstResult.glossaryTermsTreeNodes);
        expect(result.current.treeNodes).toBe(firstResult.treeNodes);

        // Re-render with new node array (same content)
        rerender({ nodes: [mockNode1], terms: [mockTerm1] });
        expect(result.current.glossaryNodesTreeNodes).not.toBe(firstResult.glossaryNodesTreeNodes);
        expect(result.current.glossaryTermsTreeNodes).not.toBe(firstResult.glossaryTermsTreeNodes);
        expect(result.current.treeNodes).not.toBe(firstResult.treeNodes);

        // Re-render with new term added
        vi.mocked(convertGlossaryTermToTreeNode).mockReturnValueOnce(mockTreeTerm1);
        vi.mocked(convertGlossaryTermToTreeNode).mockReturnValueOnce(mockTreeTerm2);
        rerender({ nodes: [mockNode1], terms: [mockTerm1, mockTerm2] });

        expect(result.current.glossaryTermsTreeNodes).toEqual([mockTreeTerm1, mockTreeTerm2]);
        expect(result.current.treeNodes).toEqual([mockTreeNode1, mockTreeTerm1, mockTreeTerm2]);
    });
});
