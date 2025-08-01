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

    const merge = (nodeA: TreeNode, nodeB: TreeNode) => {
        return {
            ...nodeA,
            ...nodeB,
            children: mergeTrees(nodeA.children ?? [], nodeB.children ?? []),
        };
    };

    const treeAValues = treeA.map((node) => node.value);
    const treeBValues = treeB.map((node) => node.value);

    // FYI: prefer original of nodes from treeA
    const finalValues = [...treeAValues, ...treeBValues.filter((value) => !treeAValues.includes(value))];

    return (
        finalValues
            .map((value) => {
                const nodeA = mapA.get(value);
                const nodeB = mapB.get(value);

                if (nodeA && nodeB) return merge(nodeA, nodeB);
                if (nodeA) return { ...nodeA };
                if (nodeB) return { ...nodeB };

                return null;
            })
            // Filter out nulls
            .filter((node): node is TreeNode => !!node)
    );
}

export function updateTree(tree: TreeNode[], nodes: TreeNode[], parentValue?: string): TreeNode[] {
    // Append nodes to root level when parent is undefined
    if (parentValue === undefined) {
        return mergeTrees(tree, nodes);
    }

    return tree.map((node) => {
        if (node.value === parentValue) {
            // Return a new node with updated children
            return {
                ...node,
                children: mergeTrees(node.children ?? [], nodes),
            };
        }

        if (node.children) {
            // Recursively update children and return a new node
            const updatedChildren = updateTree(node.children, nodes, parentValue);

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

export function updateNodeInTree(tree: TreeNode[], value: string, changes: Partial<TreeNode>): TreeNode[] {
    return tree.map((node) => {
        if (node.value === value) {
            return {
                ...node,
                ...changes,
            };
        }

        if (node.children) {
            // Recursively update children and return a new node
            const updatedChildren = updateNodeInTree(node.children, value, changes);

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
