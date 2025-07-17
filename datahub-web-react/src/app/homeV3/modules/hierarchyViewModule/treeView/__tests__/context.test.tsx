import { act, render, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { createNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/__tests__/testUtils';
import { TreeViewContextProvider, useTreeViewContext } from '@app/homeV3/modules/hierarchyViewModule/treeView/context';
import { TreeNode, TreeViewContextProviderProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    addParentValueToTreeNodes,
    getAllParentValues,
    getAllValues,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

// Mock utilities
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/utils', () => ({
    addParentValueToTreeNodes: vi.fn((nodes) => nodes),
    flattenTreeNodes: vi.fn((nodes) => nodes.flatMap((n: TreeNode) => [n, ...(n.children || [])])),
    getValueToTreeNodeMapping: vi.fn((nodes) =>
        nodes.reduce((acc: Record<string, TreeNode>, node: TreeNode) => ({ ...acc, [node.value]: node }), {}),
    ),
    getAllParentValues: vi.fn(),
    getAllValues: vi.fn(),
}));

describe('TreeViewContextProvider', () => {
    // Sample tree nodes
    const mockNodes: TreeNode[] = [
        createNode('root', 'Root', [
            createNode('child1', 'Child 1'),
            createNode('child2', 'Child 2', undefined, undefined, undefined, true),
        ]),
    ];

    const defaultProps: TreeViewContextProviderProps = {
        nodes: mockNodes,
        selectedValues: [],
        expandedValues: [],
        updateExpandedValues: vi.fn(),
        selectable: true,
        updateSelectedValues: vi.fn(),
        renderNodeLabel: (nodeProps) => <span>{nodeProps.node.label}</span>,
        explicitlySelectChildren: false,
        explicitlyUnselectChildren: false,
        explicitlySelectParent: false,
        explicitlyUnselectParent: false,
    };

    beforeEach(() => {
        vi.clearAllMocks();
        // Reset mock implementations
        vi.mocked(addParentValueToTreeNodes).mockImplementation((nodes) => nodes);
        vi.mocked(getAllParentValues).mockImplementation(() => []);
        vi.mocked(getAllValues).mockImplementation((nodes) => nodes?.map((n: TreeNode) => n.value) ?? []);
    });

    it('provides initial context values', () => {
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => <TreeViewContextProvider {...defaultProps}>{children}</TreeViewContextProvider>,
        });

        expect(result.current.nodes).toEqual(mockNodes);
        expect(result.current.getIsExpandable(mockNodes[0])).toBe(true);
        expect(result.current.getIsExpanded(mockNodes[0])).toBe(false);
    });

    it('expands and collapses nodes correctly', () => {
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => <TreeViewContextProvider {...defaultProps}>{children}</TreeViewContextProvider>,
        });

        // Expand node
        act(() => {
            result.current.expand(mockNodes[0]);
        });
        expect(result.current.getIsExpanded(mockNodes[0])).toBe(true);
        expect(defaultProps.updateExpandedValues).toHaveBeenCalledWith(['root']);

        // Collapse node
        act(() => {
            result.current.collapse(mockNodes[0]);
        });
        expect(result.current.getIsExpanded(mockNodes[0])).toBe(false);
    });

    it('toggles node expansion', () => {
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => <TreeViewContextProvider {...defaultProps}>{children}</TreeViewContextProvider>,
        });

        act(() => {
            result.current.toggleExpanded(mockNodes[0]);
        });
        expect(result.current.getIsExpanded(mockNodes[0])).toBe(true);

        act(() => {
            result.current.toggleExpanded(mockNodes[0]);
        });
        expect(result.current.getIsExpanded(mockNodes[0])).toBe(false);
    });

    it('selects and unselects nodes', () => {
        const updateSelectedValues = vi.fn();
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => (
                <TreeViewContextProvider {...defaultProps} updateSelectedValues={updateSelectedValues}>
                    {children}
                </TreeViewContextProvider>
            ),
        });

        // Select node
        act(() => {
            result.current.select(mockNodes[0]);
        });
        expect(updateSelectedValues).toHaveBeenCalledWith(['root']);

        // Unselect node
        act(() => {
            result.current.unselect(mockNodes[0]);
        });
        expect(updateSelectedValues).toHaveBeenCalledWith([]);
    });

    it('handles async children loading', () => {
        const loadChildren = vi.fn();
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => (
                <TreeViewContextProvider {...defaultProps} loadChildren={loadChildren} expandedValues={['root']}>
                    {children}
                </TreeViewContextProvider>
            ),
        });

        const asyncNode = mockNodes[0].children![1];

        // Expand node with async children
        act(() => {
            result.current.expand(asyncNode);
        });
        expect(loadChildren).toHaveBeenCalledWith(asyncNode);
        expect(result.current.getIsChildrenLoading(asyncNode)).toBe(true);
    });

    it('syncs expandedValues from props', async () => {
        // Create test component that uses the context
        const TestComponent = () => {
            const { getIsExpanded } = useTreeViewContext();
            return <div>{getIsExpanded(mockNodes[0]) ? 'Expanded' : 'Collapsed'}</div>;
        };

        // First render with expandedValues=['root']
        const { getByText, rerender } = render(
            <TreeViewContextProvider {...defaultProps} expandedValues={['root']}>
                <TestComponent />
            </TreeViewContextProvider>,
        );

        // Verify initial expanded state
        expect(getByText('Expanded')).toBeInTheDocument();

        // Rerender with empty expandedValues
        rerender(
            <TreeViewContextProvider {...defaultProps} expandedValues={[]}>
                <TestComponent />
            </TreeViewContextProvider>,
        );

        // Verify collapsed state
        await waitFor(() => {
            expect(getByText('Collapsed')).toBeInTheDocument();
        });
    });

    it('handles explicit selection flags', () => {
        const updateSelectedValues = vi.fn();
        const { result } = renderHook(() => useTreeViewContext(), {
            wrapper: ({ children }) => (
                <TreeViewContextProvider
                    {...defaultProps}
                    explicitlySelectChildren
                    explicitlyUnselectParent
                    updateSelectedValues={updateSelectedValues}
                >
                    {children}
                </TreeViewContextProvider>
            ),
        });

        act(() => {
            result.current.select(mockNodes[0]);
        });
        expect(updateSelectedValues).toHaveBeenCalledWith(['root']); // Only selects node itself

        act(() => {
            result.current.unselect(mockNodes[0]);
        });
        expect(updateSelectedValues).toHaveBeenCalledWith([]);
    });
});
