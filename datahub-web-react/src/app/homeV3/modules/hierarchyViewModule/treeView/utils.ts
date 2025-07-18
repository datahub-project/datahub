import deepmerge from 'deepmerge';

import { TreeNode, ValueToTreeNodeMap } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

export function addParentValueToTreeNodes(treeNodes: TreeNode[]): TreeNode[] {
    const addParentValue = (node: TreeNode, parentValue: string | undefined) => {
        const newNode: TreeNode = {
            ...node,
            parentValue,
        };

        if (node.children && node.children?.length) {
            newNode.children = node.children.map((child) => addParentValue(child, node.value));
        }

        return newNode;
    };

    return treeNodes.map((node) => addParentValue(node, undefined));
}

export function flattenTreeNodes(nodes?: TreeNode[]): TreeNode[] {
    if (nodes === undefined) return [];

    return nodes.reduce((flatTreeNodes: TreeNode[], node: TreeNode) => {
        flatTreeNodes.push(node);

        if (node.children?.length) {
            flatTreeNodes.push(...flattenTreeNodes(node.children));
        }
        return flatTreeNodes;
    }, []);
}

export function getValueToTreeNodeMapping(flatNodes?: TreeNode[]): ValueToTreeNodeMap {
    if (flatNodes === undefined) return {};

    return flatNodes.reduce((acc: ValueToTreeNodeMap, node: TreeNode) => {
        acc[node.value] = node;
        return acc;
    }, {});
}

export function getAllValues(nodes?: TreeNode[]): string[] {
    return (
        nodes?.reduce((acc: string[], node: TreeNode) => {
            acc.push(node.value);

            if (node.children?.length) {
                acc.push(...getAllValues(node.children));
            }

            return acc;
        }, []) ?? []
    );
}

export function getAllParentValues(node: TreeNode | undefined, valueToTreeNodeMapping: ValueToTreeNodeMap): string[] {
    if (!node?.parentValue) return [];

    const values = [node.parentValue];
    const parentNode = valueToTreeNodeMapping[node.parentValue];

    if (parentNode) {
        values.push(...getAllParentValues(parentNode, valueToTreeNodeMapping));
    }

    return values;
}

export function mergeTrees(treeA: TreeNode[], treeB: TreeNode[]): TreeNode[] {
    // Create a Map from an array
    const mapFrom = (nodes: TreeNode[]) => new Map(nodes.map((node) => [node.value, node] as const));

    const mapA = mapFrom(treeA);
    const mapB = mapFrom(treeB);

    // Merge two nodes recursively
    const mergeNodes = (nodeA: TreeNode | undefined, nodeB: TreeNode | undefined): TreeNode => {
        if (nodeA && nodeB) return deepmerge(nodeA, nodeB);
        if (!nodeA && nodeB) return { ...nodeB };
        if (!nodeB && nodeA) return { ...nodeA };

        throw new Error('Both nodes are undefined');
    };

    // Combine all unique keys from both trees
    const allKeys = Array.from(new Set([...mapA.keys(), ...mapB.keys()]));

    // Merge or pick each node accordingly
    return allKeys.map((key) => {
        const nodeA = mapA.get(key);
        const nodeB = mapB.get(key);
        return mergeNodes(nodeA, nodeB);
    });
}

export function insertChildren(nodes: TreeNode[], childrenToInsert: TreeNode[], parentValue: string): TreeNode[] {
    return nodes.map((node) => {
        if (node.value === parentValue) {
            // Return a new node with updated children
            return {
                ...node,
                children: mergeTrees(node.children ?? [], childrenToInsert),
            };
        }

        if (node.children) {
            // Recursively update children and return a new node
            const updatedChildren = insertChildren(node.children, childrenToInsert, parentValue);

            // Only create a new node if children changed
            if (updatedChildren !== node.children) {
                return {
                    ...node,
                    children: updatedChildren,
                };
            }
        }

        return node; // No change needed
    });
}

export function getTopLevelSelectedValuesFromTree(selectedValues: string[], tree: TreeNode[]): string[] {
    const result = new Set<string>();

    function traverse(node: TreeNode) {
        const isCurrentSelected = selectedValues.includes(node.value);

        if (isCurrentSelected) {
            result.add(node.value);
        } else {
            node.children?.forEach((child) => traverse(child));
        }
    }

    tree.forEach((root) => traverse(root));

    return [...result];
}
