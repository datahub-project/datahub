import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';

import { EntityType } from '@types';

// Helper function to create test tree nodes
function createTreeNode(value: string, label?: string, children?: TreeNode[], entity?: any): TreeNode {
    return {
        value,
        label: label || value,
        children,
        entity: entity || { urn: value, type: EntityType.Domain },
    };
}

describe('useTree hook', () => {
    describe('initialization', () => {
        it('should initialize with empty array when no initial tree provided', () => {
            const { result } = renderHook(() => useTree());

            expect(result.current.nodes).toEqual([]);
        });

        it('should initialize with provided tree', () => {
            const initialTree = [createTreeNode('node1'), createTreeNode('node2')];
            const { result } = renderHook(() => useTree(initialTree));

            expect(result.current.nodes).toEqual(initialTree);
        });

        it('should update nodes when initial tree prop changes', () => {
            const initialTree = [createTreeNode('node1')];
            const { result, rerender } = renderHook(({ tree }: { tree?: TreeNode[] }) => useTree(tree), {
                initialProps: { tree: initialTree },
            });

            expect(result.current.nodes).toEqual(initialTree);

            const newTree = [createTreeNode('node2')];
            rerender({ tree: newTree });

            expect(result.current.nodes).toEqual(newTree);
        });

        it('should not update nodes when tree prop is undefined after initialization', () => {
            const initialTree = [createTreeNode('node1')];
            const { result, rerender } = renderHook(({ tree }: { tree?: TreeNode[] }) => useTree(tree), {
                initialProps: { tree: initialTree },
            });

            expect(result.current.nodes).toEqual(initialTree);

            rerender({} as any);

            expect(result.current.nodes).toEqual(initialTree);
        });
    });

    describe('replace function', () => {
        it('should replace entire tree with new nodes', () => {
            const initialNodes = [createTreeNode('old')];
            const { result } = renderHook(() => useTree(initialNodes));

            const newNodes = [createTreeNode('new1'), createTreeNode('new2')];

            act(() => {
                result.current.replace(newNodes);
            });

            expect(result.current.nodes).toEqual(newNodes);
        });

        it('should replace with empty array', () => {
            const initialNodes = [createTreeNode('node')];
            const { result } = renderHook(() => useTree(initialNodes));

            act(() => {
                result.current.replace([]);
            });

            expect(result.current.nodes).toEqual([]);
        });
    });

    describe('merge function', () => {
        it('should merge new nodes with existing tree', () => {
            const existingNodes = [createTreeNode('existing')];
            const { result } = renderHook(() => useTree(existingNodes));

            const nodesToMerge = [createTreeNode('new')];

            act(() => {
                result.current.merge(nodesToMerge);
            });

            expect(result.current.nodes).toEqual([...existingNodes, ...nodesToMerge]);
        });

        it('should merge overlapping nodes correctly', () => {
            const existingNode = createTreeNode('shared', 'original label');
            const initialNodes = [existingNode];
            const { result } = renderHook(() => useTree(initialNodes));

            const nodeToMerge = createTreeNode('shared', 'updated label');

            act(() => {
                result.current.merge([nodeToMerge]);
            });

            expect(result.current.nodes).toHaveLength(1);
            expect(result.current.nodes[0]).toEqual({
                ...existingNode,
                ...nodeToMerge,
                children: [],
            });
        });

        it('should merge children recursively', () => {
            const existingChild = createTreeNode('existingChild');
            const existingParent = createTreeNode('parent', 'parent', [existingChild]);
            const initialNodes = [existingParent];
            const { result } = renderHook(() => useTree(initialNodes));

            const newChild = createTreeNode('newChild');
            const nodeToMerge = createTreeNode('parent', 'parent', [newChild]);

            act(() => {
                result.current.merge([nodeToMerge]);
            });

            expect(result.current.nodes).toHaveLength(1);
            expect(result.current.nodes[0].children).toEqual([existingChild, newChild]);
        });
    });

    describe('update function', () => {
        it('should append to root when no parent specified', () => {
            const existingNodes = [createTreeNode('existing')];
            const { result } = renderHook(() => useTree(existingNodes));

            const newNodes = [createTreeNode('new')];

            act(() => {
                result.current.update(newNodes);
            });

            expect(result.current.nodes).toEqual([...existingNodes, ...newNodes]);
        });

        it('should update children of specified parent', () => {
            const child = createTreeNode('existingChild');
            const parent = createTreeNode('parent', 'parent', [child]);
            const initialNodes = [parent];
            const { result } = renderHook(() => useTree(initialNodes));

            const newChildren = [createTreeNode('newChild')];

            act(() => {
                result.current.update(newChildren, 'parent');
            });

            expect(result.current.nodes[0].children).toEqual([child, ...newChildren]);
        });

        it('should not modify tree when parent not found', () => {
            const originalNodes = [createTreeNode('node')];
            const { result } = renderHook(() => useTree(originalNodes));

            const newNodes = [createTreeNode('new')];

            act(() => {
                result.current.update(newNodes, 'nonexistent');
            });

            expect(result.current.nodes).toEqual(originalNodes);
        });

        it('should update nested children correctly', () => {
            const grandchild = createTreeNode('grandchild');
            const child = createTreeNode('child', 'child', [grandchild]);
            const parent = createTreeNode('parent', 'parent', [child]);
            const initialNodes = [parent];
            const { result } = renderHook(() => useTree(initialNodes));

            const newGrandchildren = [createTreeNode('newGrandchild')];

            act(() => {
                result.current.update(newGrandchildren, 'child');
            });

            expect(result.current.nodes[0].children?.[0].children).toEqual([grandchild, ...newGrandchildren]);
        });
    });

    describe('updateNode function', () => {
        it('should update node with matching value', () => {
            const node = createTreeNode('target', 'original');
            const initialNodes = [node];
            const { result } = renderHook(() => useTree(initialNodes));

            const changes = { label: 'updated' };

            act(() => {
                result.current.updateNode('target', changes);
            });

            expect(result.current.nodes[0]).toEqual({ ...node, ...changes });
        });

        it('should not modify other nodes', () => {
            const node1 = createTreeNode('node1');
            const node2 = createTreeNode('node2');
            const initialNodes = [node1, node2];
            const { result } = renderHook(() => useTree(initialNodes));

            const changes = { label: 'updated' };

            act(() => {
                result.current.updateNode('node1', changes);
            });

            expect(result.current.nodes[0]).toEqual({ ...node1, ...changes });
            expect(result.current.nodes[1]).toEqual(node2);
        });

        it('should update nested nodes', () => {
            const child = createTreeNode('child', 'original');
            const parent = createTreeNode('parent', 'parent', [child]);
            const initialNodes = [parent];
            const { result } = renderHook(() => useTree(initialNodes));

            const changes = { label: 'updated child' };

            act(() => {
                result.current.updateNode('child', changes);
            });

            expect(result.current.nodes[0].children?.[0]).toEqual({ ...child, ...changes });
        });

        it('should return original tree when node not found', () => {
            const originalNodes = [createTreeNode('node')];
            const { result } = renderHook(() => useTree(originalNodes));

            const changes = { label: 'updated' };

            act(() => {
                result.current.updateNode('nonexistent', changes);
            });

            expect(result.current.nodes).toEqual(originalNodes);
        });

        it('should update multiple properties at once', () => {
            const node = createTreeNode('target');
            const initialNodes = [node];
            const { result } = renderHook(() => useTree(initialNodes));

            const changes = {
                label: 'updated label',
                isChildrenLoading: true,
                totalChildren: 5,
            };

            act(() => {
                result.current.updateNode('target', changes);
            });

            expect(result.current.nodes).toEqual([{ ...node, ...changes }]);
        });
    });

    describe('immutability', () => {
        it('should not mutate original tree on replace', () => {
            const originalTree = [createTreeNode('original')];
            const { result } = renderHook(() => useTree(originalTree));

            const newTree = [createTreeNode('new')];

            act(() => {
                result.current.replace(newTree);
            });

            expect(originalTree).toEqual([createTreeNode('original')]);
        });

        it('should not mutate original tree on merge', () => {
            const originalTree = [createTreeNode('original')];
            const { result } = renderHook(() => useTree(originalTree));

            const treeToMerge = [createTreeNode('merged')];

            act(() => {
                result.current.merge(treeToMerge);
            });

            expect(originalTree).toEqual([createTreeNode('original')]);
        });

        it('should not mutate original tree on update', () => {
            const child = createTreeNode('child');
            const parent = createTreeNode('parent', 'parent', [child]);
            const originalTree = [parent];
            const { result } = renderHook(() => useTree(originalTree));

            const newNodes = [createTreeNode('new')];

            act(() => {
                result.current.update(newNodes, 'parent');
            });

            expect(originalTree[0].children).toEqual([child]);
        });

        it('should not mutate original tree on updateNode', () => {
            const originalNode = createTreeNode('node', 'original');
            const originalTree = [originalNode];
            const { result } = renderHook(() => useTree(originalTree));

            const changes = { label: 'updated' };

            act(() => {
                result.current.updateNode('node', changes);
            });

            expect(originalNode.label).toBe('original');
        });
    });
});
