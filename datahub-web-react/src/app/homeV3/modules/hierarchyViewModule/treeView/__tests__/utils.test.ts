import { describe, expect, it } from 'vitest';

import { createNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/__tests__/testUtils';
import {
    addParentValueToTreeNodes,
    flattenTreeNodes,
    getAllParentValues,
    getAllValues,
    getTopLevelSelectedValuesFromTree,
    getValueToTreeNodeMapping,
    insertChildren,
    mergeTrees,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

import { EntityType } from '@types';

describe('addParentValueToTreeNodes', () => {
    it('should assign parentValue correctly', () => {
        const tree = [createNode('1', 'Root', [createNode('2', 'Child')])];

        const result = addParentValueToTreeNodes(tree);
        expect(result[0].children?.[0].parentValue).toBe('1');
    });
});

describe('flattenTreeNodes', () => {
    it('should flatten nested nodes into single array', () => {
        const tree = [createNode('1', 'Root', [createNode('2', 'Child', [createNode('3', 'Grandchild')])])];

        const result = flattenTreeNodes(tree);
        expect(result.map((n) => n.value)).toEqual(['1', '2', '3']);
    });

    it('should return empty array if input is undefined', () => {
        expect(flattenTreeNodes(undefined)).toEqual([]);
    });
});

describe('getValueToTreeNodeMapping', () => {
    it('should map values to nodes correctly', () => {
        const flatNodes = [createNode('1', 'One'), createNode('2', 'Two')];
        const result = getValueToTreeNodeMapping(flatNodes);

        expect(Object.keys(result)).toEqual(['1', '2']);
        expect(result['1'].label).toBe('One');
    });

    it('should return empty object if input is undefined', () => {
        expect(getValueToTreeNodeMapping(undefined)).toEqual({});
    });
});

describe('getAllValues', () => {
    it('should collect all node values recursively', () => {
        const nodes = [createNode('1', 'One', [createNode('2', 'Two')])];

        expect(getAllValues(nodes)).toEqual(['1', '2']);
    });

    it('should return empty array if input is undefined', () => {
        expect(getAllValues(undefined)).toEqual([]);
    });
});

describe('getAllParentValues', () => {
    it('should return all ancestor values', () => {
        const flatTree = [
            createNode('grandparent', 'G'),
            createNode('parent', 'P', [], 'grandparent'),
            createNode('child', 'C', [], 'parent'),
        ];

        const mapping = getValueToTreeNodeMapping(flatTree);
        const childNode = mapping.child;

        expect(getAllParentValues(childNode, mapping)).toEqual(['parent', 'grandparent']);
    });
});

describe('mergeTrees', () => {
    it('should deep merge two trees by value', () => {
        const treeA = [createNode('a', 'A', [], undefined, EntityType.Chart)];
        const treeB = [createNode('a', 'A', [], undefined, EntityType.Dataset)];

        const merged = mergeTrees(treeA, treeB);
        expect(merged[0].entity.type).toBe(EntityType.Dataset); // B overwrites A
    });

    it('should combine unique nodes from both trees', () => {
        const treeA = [createNode('a', 'A')];
        const treeB = [createNode('b', 'B')];

        const merged = mergeTrees(treeA, treeB);
        expect(merged.length).toBe(2);
    });
});

describe('insertChildren', () => {
    it('should insert children under correct parent', () => {
        const tree = [createNode('a', 'A')];
        const newChild = [createNode('b', 'B')];

        const result = insertChildren(tree, newChild, 'a');

        expect(result[0].children?.[0].value).toBe('b');
    });

    it('should not modify unrelated nodes', () => {
        const tree = [createNode('a', 'A', [createNode('c', 'C')])];
        const newChild = [createNode('b', 'B')];

        const result = insertChildren(tree, newChild, 'a');
        expect(result[0].children?.length).toBe(2);
        expect(result[0].children?.[1].value).toBe('b');
    });
});

describe('getTopLevelSelectedValuesFromTree', () => {
    it('should collect top-level selected nodes', () => {
        const tree = [createNode('a', 'A', [createNode('b', 'B', [createNode('c', 'C', [createNode('d', 'D')])])])];

        const selected = ['b', 'd'];
        const result = getTopLevelSelectedValuesFromTree(selected, tree);
        expect(result).toEqual(['b']); // Only direct match at root level
    });

    it('should include root if selected', () => {
        const tree = [createNode('a', 'A')];
        const selected = ['a'];
        const result = getTopLevelSelectedValuesFromTree(selected, tree);
        expect(result).toEqual(['a']);
    });
});
