import { describe, expect, it } from 'vitest';

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    addParentValueToTreeNodes,
    flattenTreeNodes,
    getAllParentValues,
    getAllValues,
    getTopLevelSelectedValuesFromTree,
    getValueToTreeNodeMapping,
    mergeTrees,
    updateNodeInTree,
    updateTree,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

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

describe('treeView utils', () => {
    describe('addParentValueToTreeNodes', () => {
        it('should add parentValue to root nodes as undefined', () => {
            const nodes = [createTreeNode('root1'), createTreeNode('root2')];

            const result = addParentValueToTreeNodes(nodes);

            expect(result).toEqual([
                { ...nodes[0], parentValue: undefined },
                { ...nodes[1], parentValue: undefined },
            ]);
        });

        it('should add parentValue to nested nodes correctly', () => {
            const child1 = createTreeNode('child1');
            const child2 = createTreeNode('child2');
            const root = createTreeNode('root', 'root', [child1, child2]);

            const result = addParentValueToTreeNodes([root]);

            expect(result[0]).toEqual({
                ...root,
                parentValue: undefined,
                children: [
                    { ...child1, parentValue: 'root' },
                    { ...child2, parentValue: 'root' },
                ],
            });
        });

        it('should handle deeply nested trees', () => {
            const grandchild = createTreeNode('grandchild');
            const child = createTreeNode('child', 'child', [grandchild]);
            const root = createTreeNode('root', 'root', [child]);

            const result = addParentValueToTreeNodes([root]);

            expect(result[0]).toEqual({
                ...root,
                parentValue: undefined,
                children: [
                    {
                        ...child,
                        parentValue: 'root',
                        children: [
                            {
                                ...grandchild,
                                parentValue: 'child',
                            },
                        ],
                    },
                ],
            });
        });
    });

    describe('flattenTreeNodes', () => {
        it('should return empty array for undefined nodes', () => {
            const result = flattenTreeNodes(undefined);
            expect(result).toEqual([]);
        });

        it('should return flat array for nodes without children', () => {
            const nodes = [createTreeNode('node1'), createTreeNode('node2')];

            const result = flattenTreeNodes(nodes);

            expect(result).toEqual(nodes);
        });

        it('should flatten nested tree structure', () => {
            const child1 = createTreeNode('child1');
            const child2 = createTreeNode('child2');
            const root = createTreeNode('root', 'root', [child1, child2]);

            const result = flattenTreeNodes([root]);

            expect(result).toEqual([root, child1, child2]);
        });

        it('should flatten deeply nested structures', () => {
            const grandchild = createTreeNode('grandchild');
            const child = createTreeNode('child', 'child', [grandchild]);
            const root = createTreeNode('root', 'root', [child]);

            const result = flattenTreeNodes([root]);

            expect(result).toEqual([root, child, grandchild]);
        });
    });

    describe('getValueToTreeNodeMapping', () => {
        it('should return empty object for undefined nodes', () => {
            const result = getValueToTreeNodeMapping(undefined);
            expect(result).toEqual({});
        });

        it('should create mapping from flat array', () => {
            const nodes = [createTreeNode('node1'), createTreeNode('node2')];

            const result = getValueToTreeNodeMapping(nodes);

            expect(result).toEqual({
                node1: nodes[0],
                node2: nodes[1],
            });
        });
    });

    describe('getAllValues', () => {
        it('should return empty array for undefined nodes', () => {
            const result = getAllValues(undefined);
            expect(result).toEqual([]);
        });

        it('should return values for flat nodes', () => {
            const nodes = [createTreeNode('node1'), createTreeNode('node2')];

            const result = getAllValues(nodes);

            expect(result).toEqual(['node1', 'node2']);
        });

        it('should return all values from nested structure', () => {
            const child1 = createTreeNode('child1');
            const child2 = createTreeNode('child2');
            const root = createTreeNode('root', 'root', [child1, child2]);

            const result = getAllValues([root]);

            expect(result).toEqual(['root', 'child1', 'child2']);
        });
    });

    describe('getAllParentValues', () => {
        it('should return empty array for node without parent', () => {
            const node = createTreeNode('node');
            const mapping = {};

            const result = getAllParentValues(node, mapping);

            expect(result).toEqual([]);
        });

        it('should return empty array for undefined node', () => {
            const result = getAllParentValues(undefined, {});
            expect(result).toEqual([]);
        });

        it('should return single parent value', () => {
            const parent = createTreeNode('parent');
            const node = { ...createTreeNode('child'), parentValue: 'parent' };
            const mapping = { parent };

            const result = getAllParentValues(node, mapping);

            expect(result).toEqual(['parent']);
        });

        it('should return all parent values up the chain', () => {
            const grandparent = createTreeNode('grandparent');
            const parent = { ...createTreeNode('parent'), parentValue: 'grandparent' };
            const node = { ...createTreeNode('child'), parentValue: 'parent' };
            const mapping = {
                grandparent,
                parent,
            };

            const result = getAllParentValues(node, mapping);

            expect(result).toEqual(['parent', 'grandparent']);
        });
    });

    describe('mergeTrees', () => {
        it('should merge two empty trees', () => {
            const result = mergeTrees([], []);
            expect(result).toEqual([]);
        });

        it('should return first tree when second is empty', () => {
            const treeA = [createTreeNode('node1')];

            const result = mergeTrees(treeA, []);

            expect(result).toEqual(treeA);
        });

        it('should return second tree when first is empty', () => {
            const treeB = [createTreeNode('node1')];

            const result = mergeTrees([], treeB);

            expect(result).toEqual(treeB);
        });

        it('should merge trees with different nodes', () => {
            const treeA = [createTreeNode('nodeA')];
            const treeB = [createTreeNode('nodeB')];

            const result = mergeTrees(treeA, treeB);

            expect(result).toEqual([...treeA, ...treeB]);
        });

        it('should merge overlapping nodes', () => {
            const nodeA = createTreeNode('shared', 'labelA');
            const nodeB = createTreeNode('shared', 'labelB');

            const result = mergeTrees([nodeA], [nodeB]);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                ...nodeA,
                ...nodeB,
                children: [],
            });
        });

        it('should merge children recursively', () => {
            const childA = createTreeNode('childA');
            const childB = createTreeNode('childB');
            // Create separate objects with same value to avoid object reference issues
            const sharedChildA = createTreeNode('shared');
            const sharedChildB = createTreeNode('shared');

            const nodeA = createTreeNode('root', 'root', [childA, sharedChildA]);
            const nodeB = createTreeNode('root', 'root', [childB, sharedChildB]);

            const result = mergeTrees([nodeA], [nodeB]);

            expect(result).toHaveLength(1);
            expect(result[0].value).toBe('root');
            // The merge should include all children, with shared child appearing once
            expect(result[0].children).toHaveLength(3);
            expect(result[0].children?.map((n) => n.value)).toContain('childA');
            expect(result[0].children?.map((n) => n.value)).toContain('childB');
            expect(result[0].children?.map((n) => n.value)).toContain('shared');
        });
    });

    describe('updateTree', () => {
        it('should append to root when parentValue is undefined', () => {
            const tree = [createTreeNode('existing')];
            const newNodes = [createTreeNode('new')];

            const result = updateTree(tree, newNodes, undefined);

            expect(result).toEqual([...tree, ...newNodes]);
        });

        it('should update children of matching parent', () => {
            const child = createTreeNode('existingChild');
            const parent = createTreeNode('parent', 'parent', [child]);
            const tree = [parent];
            const newNodes = [createTreeNode('newChild')];

            const result = updateTree(tree, newNodes, 'parent');

            expect(result[0].children).toEqual([child, ...newNodes]);
        });

        it('should not modify tree when parent not found', () => {
            const tree = [createTreeNode('node')];
            const newNodes = [createTreeNode('new')];

            const result = updateTree(tree, newNodes, 'nonexistent');

            expect(result).toEqual(tree);
        });

        it('should update nested children correctly', () => {
            const grandchild = createTreeNode('grandchild');
            const child = createTreeNode('child', 'child', [grandchild]);
            const root = createTreeNode('root', 'root', [child]);
            const tree = [root];
            const newNodes = [createTreeNode('newGrandchild')];

            const result = updateTree(tree, newNodes, 'child');

            expect(result[0].children?.[0].children).toEqual([grandchild, ...newNodes]);
        });
    });

    describe('updateNodeInTree', () => {
        it('should update node with matching value', () => {
            const node = createTreeNode('target');
            const tree = [node];
            const changes = { label: 'updated label' };

            const result = updateNodeInTree(tree, 'target', changes);

            expect(result[0]).toEqual({ ...node, ...changes });
        });

        it('should not modify other nodes', () => {
            const node1 = createTreeNode('node1');
            const node2 = createTreeNode('node2');
            const tree = [node1, node2];
            const changes = { label: 'updated' };

            const result = updateNodeInTree(tree, 'node1', changes);

            expect(result[0]).toEqual({ ...node1, ...changes });
            expect(result[1]).toEqual(node2);
        });

        it('should update nested nodes', () => {
            const child = createTreeNode('child');
            const parent = createTreeNode('parent', 'parent', [child]);
            const tree = [parent];
            const changes = { label: 'updated child' };

            const result = updateNodeInTree(tree, 'child', changes);

            expect(result[0].children?.[0]).toEqual({ ...child, ...changes });
        });

        it('should return original tree when node not found', () => {
            const tree = [createTreeNode('node')];
            const changes = { label: 'updated' };

            const result = updateNodeInTree(tree, 'nonexistent', changes);

            expect(result).toEqual(tree);
        });
    });

    describe('getTopLevelSelectedValuesFromTree', () => {
        it('should return empty array when no values selected', () => {
            const tree = [createTreeNode('root')];

            const result = getTopLevelSelectedValuesFromTree([], tree);

            expect(result).toEqual([]);
        });

        it('should return selected root nodes', () => {
            const tree = [createTreeNode('root1'), createTreeNode('root2')];
            const selectedValues = ['root1'];

            const result = getTopLevelSelectedValuesFromTree(selectedValues, tree);

            expect(result).toEqual(['root1']);
        });

        it('should return parent when parent is selected', () => {
            const child = createTreeNode('child');
            const parent = createTreeNode('parent', 'parent', [child]);
            const tree = [parent];
            const selectedValues = ['parent'];

            const result = getTopLevelSelectedValuesFromTree(selectedValues, tree);

            expect(result).toEqual(['parent']);
        });

        it('should return child when only child is selected', () => {
            const child = createTreeNode('child');
            const parent = createTreeNode('parent', 'parent', [child]);
            const tree = [parent];
            const selectedValues = ['child'];

            const result = getTopLevelSelectedValuesFromTree(selectedValues, tree);

            expect(result).toEqual(['child']);
        });

        it('should prefer parent over children when both selected', () => {
            const child1 = createTreeNode('child1');
            const child2 = createTreeNode('child2');
            const parent = createTreeNode('parent', 'parent', [child1, child2]);
            const tree = [parent];
            const selectedValues = ['parent', 'child1', 'child2'];

            const result = getTopLevelSelectedValuesFromTree(selectedValues, tree);

            expect(result).toEqual(['parent']);
        });

        it('should handle multiple trees correctly', () => {
            const child1 = createTreeNode('child1');
            const child2 = createTreeNode('child2');
            const parent1 = createTreeNode('parent1', 'parent1', [child1]);
            const parent2 = createTreeNode('parent2', 'parent2', [child2]);
            const tree = [parent1, parent2];
            const selectedValues = ['parent1', 'child2'];

            const result = getTopLevelSelectedValuesFromTree(selectedValues, tree);

            expect(result).toEqual(['parent1', 'child2']);
        });
    });
});
