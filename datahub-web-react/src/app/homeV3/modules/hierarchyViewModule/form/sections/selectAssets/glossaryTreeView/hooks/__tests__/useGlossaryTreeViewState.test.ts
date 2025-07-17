import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useGlossaryTreeViewState from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryTreeViewState';
import useInitialGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useInitialGlossaryNodesAndTerms';
import useRootGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodesAndTerms';
import useTreeNodesFromFlatGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromFlatGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

// Mock dependencies
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useInitialGlossaryNodesAndTerms',
);
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useRootGlossaryNodesAndTerms',
);
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromFlatGlossaryNodesAndTerms',
);
vi.mock(
    '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodesAndTerms',
);
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/utils', () => ({
    mergeTrees: vi.fn(),
}));

describe('useGlossaryTreeViewState', () => {
    const mockInitialNodes = [
        { value: 'initial1', label: 'Initial 1' },
        { value: 'initial2', label: 'Initial 2' },
    ] as TreeNode[];

    const mockRootNodes = [
        { value: 'root1', label: 'Root 1' },
        { value: 'root2', label: 'Root 2' },
    ] as TreeNode[];

    const mockMergedNodes = [
        { value: 'merged1', label: 'Merged 1' },
        { value: 'merged2', label: 'Merged 2' },
    ] as TreeNode[];

    // Mock return values
    const mockInitialHook = vi.fn();
    const mockRootHook = vi.fn();
    const mockTreeNodesFromFlatHook = vi.fn();
    const mockTreeNodesFromHook = vi.fn();
    const mockMergeTrees = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();

        // Setup default mocks
        mockInitialHook.mockReturnValue({
            glossaryNodes: undefined,
            glossaryTerms: undefined,
        });

        mockRootHook.mockReturnValue({
            glossaryNodes: undefined,
            glossaryTerms: undefined,
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: [],
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: [],
        });

        mockMergeTrees.mockReturnValue([]);

        // Assign mocks to specific hooks
        vi.mocked(useInitialGlossaryNodesAndTerms).mockImplementation(() => mockInitialHook());

        vi.mocked(useRootGlossaryNodesAndTerms).mockImplementation(() => mockRootHook());

        vi.mocked(useTreeNodesFromFlatGlossaryNodesAndTerms).mockImplementation(() => mockTreeNodesFromFlatHook());

        vi.mocked(useTreeNodesFromGlossaryNodesAndTerms).mockImplementation(() => mockTreeNodesFromHook());

        vi.mocked(mergeTrees).mockImplementation(mockMergeTrees);
    });

    it('initializes with correct initial state', () => {
        const initialUrns = ['urn1', 'urn2'];
        const { result } = renderHook(() => useGlossaryTreeViewState(initialUrns));

        expect(result.current).toEqual({
            nodes: [],
            setNodes: expect.any(Function),
            selectedValues: initialUrns,
            setSelectedValues: expect.any(Function),
            loading: true,
        });
    });

    it('sets initial selectedValues', () => {
        const initialUrns = ['urn1', 'urn2'];
        const { result } = renderHook(() => useGlossaryTreeViewState(initialUrns));
        expect(result.current.selectedValues).toEqual(initialUrns);
    });

    it('does not initialize when data is not ready', () => {
        // Keep initial data undefined
        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        expect(result.current.loading).toBe(true);
        expect(result.current.nodes).toEqual([]);
        expect(mockMergeTrees).not.toHaveBeenCalled();
    });

    it('initializes when all data becomes available', () => {
        // Set up mock data
        mockInitialHook.mockReturnValue({
            glossaryNodes: ['node1', 'node2'],
            glossaryTerms: ['term1', 'term2'],
        });

        mockRootHook.mockReturnValue({
            glossaryNodes: ['rootNode1', 'rootNode2'],
            glossaryTerms: ['rootTerm1', 'rootTerm2'],
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: mockInitialNodes,
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: mockRootNodes,
        });

        mockMergeTrees.mockReturnValue(mockMergedNodes);

        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        // Should initialize
        expect(result.current.loading).toBe(false);
        expect(result.current.nodes).toEqual(mockMergedNodes);
        expect(mockMergeTrees).toHaveBeenCalledWith(mockRootNodes, mockInitialNodes);
    });

    it('only initializes once', () => {
        // Set up mock data
        mockInitialHook.mockReturnValue({
            glossaryNodes: ['node1'],
            glossaryTerms: ['term1'],
        });

        mockRootHook.mockReturnValue({
            glossaryNodes: ['rootNode1'],
            glossaryTerms: ['rootTerm1'],
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: mockInitialNodes,
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: mockRootNodes,
        });

        const { result, rerender } = renderHook(() => useGlossaryTreeViewState([]));

        // Should initialize
        expect(result.current.loading).toBe(false);
        const initialCallCount = mockMergeTrees.mock.calls.length;

        // Change mock data
        mockInitialHook.mockReturnValue({
            glossaryNodes: ['node2'],
            glossaryTerms: ['term2'],
        });

        // Rerender
        rerender();

        // Should not re-initialize
        expect(result.current.loading).toBe(false);
        expect(mockMergeTrees.mock.calls.length).toBe(initialCallCount);
    });

    it('allows updating nodes and selectedValues', () => {
        const initialUrns = ['urn1'];
        const { result } = renderHook(() => useGlossaryTreeViewState(initialUrns));

        // Update nodes
        act(() => {
            result.current.setNodes(mockMergedNodes);
        });

        expect(result.current.nodes).toEqual(mockMergedNodes);

        // Update selectedValues
        const newSelectedValues = ['urn1', 'urn2'];
        act(() => {
            result.current.setSelectedValues(newSelectedValues);
        });

        expect(result.current.selectedValues).toEqual(newSelectedValues);
    });

    it('handles partial data availability', () => {
        // Only root data available
        mockRootHook.mockReturnValue({
            glossaryNodes: ['rootNode1'],
            glossaryTerms: ['rootTerm1'],
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: mockRootNodes,
        });

        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        // Should not initialize without initial data
        expect(result.current.loading).toBe(true);
        expect(result.current.nodes).toEqual([]);
    });

    it('handles empty initial URNs', () => {
        // Set up mock data
        mockInitialHook.mockReturnValue({
            glossaryNodes: [],
            glossaryTerms: [],
        });

        mockRootHook.mockReturnValue({
            glossaryNodes: ['rootNode1'],
            glossaryTerms: ['rootTerm1'],
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: [],
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: mockRootNodes,
        });

        mockMergeTrees.mockReturnValue(mockRootNodes);

        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        // Should initialize with root nodes
        expect(result.current.loading).toBe(false);
        expect(result.current.nodes).toEqual(mockRootNodes);
    });

    it('does not merge trees when not initialized', () => {
        // Only provide partial data
        mockInitialHook.mockReturnValue({
            glossaryNodes: ['node1'],
            glossaryTerms: ['term1'],
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: mockInitialNodes,
        });

        // Root data is undefined
        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        expect(result.current.loading).toBe(true);
        expect(result.current.nodes).toEqual([]);
        expect(mockMergeTrees).not.toHaveBeenCalled();
    });

    it('handles empty merged trees', () => {
        // Set up mock data
        mockInitialHook.mockReturnValue({
            glossaryNodes: ['node1'],
            glossaryTerms: ['term1'],
        });

        mockRootHook.mockReturnValue({
            glossaryNodes: ['rootNode1'],
            glossaryTerms: ['rootTerm1'],
        });

        mockTreeNodesFromFlatHook.mockReturnValue({
            treeNodes: [],
        });

        mockTreeNodesFromHook.mockReturnValue({
            treeNodes: [],
        });

        mockMergeTrees.mockReturnValue([]);

        const { result } = renderHook(() => useGlossaryTreeViewState([]));

        // Should initialize with empty nodes
        expect(result.current.loading).toBe(false);
        expect(result.current.nodes).toEqual([]);
    });
});
