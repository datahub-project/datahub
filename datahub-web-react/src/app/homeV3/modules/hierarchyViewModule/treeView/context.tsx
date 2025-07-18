import React, { useCallback, useContext, useEffect, useMemo, useState } from 'react';

import {
    TreeNode,
    TreeViewContextProviderProps,
    TreeViewContextType,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    addParentValueToTreeNodes,
    flattenTreeNodes,
    getAllParentValues,
    getAllValues,
    getValueToTreeNodeMapping,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const DEFAULT_TREE_VIEW_CONTEXT: TreeViewContextType = {
    nodes: [],

    getHasParentNode: () => false,

    getIsExpandable: () => false,
    getIsExpanded: () => false,
    expand: () => {},
    collapse: () => {},
    toggleExpanded: () => {},

    getIsSelectable: () => false,
    getIsSelected: () => false,
    getIsParentSelected: () => false,
    getHasSelectedChildren: () => false,
    select: () => {},
    unselect: () => {},
    toggleSelected: () => {},
    getIsChildrenLoading: () => false,
    explicitlySelectChildren: false,
    explicitlyUnselectChildren: false,
    explicitlySelectParent: false,
    explicitlyUnselectParent: false,
};

const TreeViewContext = React.createContext<TreeViewContextType>(DEFAULT_TREE_VIEW_CONTEXT);

export function useTreeViewContext() {
    return useContext<TreeViewContextType>(TreeViewContext);
}

export function TreeViewContextProvider({
    children,
    nodes,
    selectedValues,
    expandedValues,
    updateExpandedValues,
    selectable,
    updateSelectedValues,
    loadChildren: loadAsyncChildren,
    renderNodeLabel,
    explicitlySelectChildren,
    explicitlyUnselectChildren,
    explicitlySelectParent,
    explicitlyUnselectParent,
}: React.PropsWithChildren<TreeViewContextProviderProps>) {
    const [internalExpandedValues, setInternalExpandedValues] = useState<string[]>(expandedValues ?? []);
    const [loadedValues, setLoadedValues] = useState<string[]>([]);

    const preprocessedNodes = useMemo(() => {
        return addParentValueToTreeNodes(nodes);
    }, [nodes]);

    const flatTreeNodes = useMemo(() => flattenTreeNodes(preprocessedNodes), [preprocessedNodes]);
    const valueToTreeNodeMap = useMemo(() => getValueToTreeNodeMapping(flatTreeNodes), [flatTreeNodes]);

    const loadAsyncChildrenHandler = useCallback(
        (node: TreeNode) => {
            if (!loadAsyncChildren) return null;
            if (!node.hasAsyncChildren) return null;
            if (loadedValues.includes(node.value)) return null;

            loadAsyncChildren(node);
            setLoadedValues((prev) => [...prev, node.value]);
            return null;
        },
        [loadAsyncChildren, loadedValues],
    );

    const getHasParentNode = useCallback((node: TreeNode) => !!node.parentValue, []);

    // Expanding

    const getIsExpandable = useCallback((node: TreeNode) => !!node.children?.length || !!node.hasAsyncChildren, []);

    const getIsExpanded = useCallback(
        (node: TreeNode) => {
            const valuesToCheck = getAllValues([node]);
            return valuesToCheck.some((value) => internalExpandedValues.includes(value));
        },
        [internalExpandedValues],
    );

    const expand = useCallback(
        (node: TreeNode) => {
            loadAsyncChildrenHandler(node); // try to load children on expand
            const parentValues = getAllParentValues(node, valueToTreeNodeMap);
            const newExpandedValues = [...internalExpandedValues, ...parentValues, node.value];
            setInternalExpandedValues(newExpandedValues);
            updateExpandedValues?.(newExpandedValues);
        },
        [loadAsyncChildrenHandler, updateExpandedValues, valueToTreeNodeMap, internalExpandedValues],
    );

    const collapse = useCallback(
        (node: TreeNode) => {
            const values = getAllValues([node]);
            const newExpandedValues = internalExpandedValues.filter((value) => !values.includes(value)) ?? [];
            setInternalExpandedValues(newExpandedValues);
            updateExpandedValues?.(newExpandedValues);
        },
        [internalExpandedValues, updateExpandedValues],
    );

    const toggleExpanded = useCallback(
        (node: TreeNode) => {
            const isExpanded = getIsExpanded(node);

            if (isExpanded) {
                collapse(node);
            } else {
                expand(node);
            }
        },
        [getIsExpanded, expand, collapse],
    );

    // Sync internal expanded values
    useEffect(() => {
        if (expandedValues !== undefined) setInternalExpandedValues(expandedValues);
    }, [expandedValues]);

    // Load children of initial expanded nodes
    useEffect(() => {
        expandedValues
            ?.map((value) => valueToTreeNodeMap[value])
            .filter((node): node is TreeNode => !!node)
            .flatMap((node) => [...getAllParentValues(node, valueToTreeNodeMap), node.value])
            .map((value) => valueToTreeNodeMap[value])
            .filter((node): node is TreeNode => !!node)
            .forEach(loadAsyncChildrenHandler);
    }, [expandedValues, valueToTreeNodeMap, loadAsyncChildrenHandler]);

    useEffect(() => {
        if (expandedValues) setInternalExpandedValues(expandedValues);
    }, [expandedValues]);

    // SELECTING
    const getIsSelectable = useCallback(
        (node: TreeNode) => {
            if (!selectable) return false;
            return node.selectable === undefined || node.selectable;
        },
        [selectable],
    );

    const getIsSelected = useCallback((node: TreeNode) => !!selectedValues?.includes(node.value), [selectedValues]);

    const getIsParentSelected = useCallback(
        (node: TreeNode) => {
            return !!node.parentValue && !!selectedValues?.includes(node.parentValue);
        },
        [selectedValues],
    );

    const getHasSelectedChildren = useCallback(
        (node: TreeNode) => {
            const childrenValues = getAllValues(node.children);
            return childrenValues.some((value) => selectedValues?.includes(value));
        },
        [selectedValues],
    );

    const select = useCallback(
        (node: TreeNode) => {
            const valuesToToggleSelect = explicitlySelectChildren ? [node.value] : getAllValues([node]);
            const parentValues = explicitlySelectParent
                ? []
                : getAllParentValues(valueToTreeNodeMap[node.value], valueToTreeNodeMap);
            const newSelectedValues = [...(selectedValues ?? []), ...valuesToToggleSelect];

            parentValues.forEach((parentValue) => {
                const parentNode = valueToTreeNodeMap[parentValue];
                if (!parentNode) return null;

                const childrenValues = getAllValues(parentNode.children);
                const isAllChildrenSelected = childrenValues.every((childValue) =>
                    newSelectedValues.includes(childValue),
                );
                if (isAllChildrenSelected) {
                    newSelectedValues.push(parentValue);
                }

                return null;
            });

            updateSelectedValues?.(newSelectedValues);
        },
        [selectedValues, valueToTreeNodeMap, updateSelectedValues, explicitlySelectChildren, explicitlySelectParent],
    );

    const unselect = useCallback(
        (node: TreeNode) => {
            const valuesToToggleSelect = explicitlyUnselectChildren ? [node.value] : getAllValues([node]);
            const parentValues = explicitlyUnselectParent
                ? []
                : getAllParentValues(valueToTreeNodeMap[node.value], valueToTreeNodeMap);
            updateSelectedValues?.(
                selectedValues?.filter(
                    (value) => !parentValues.includes(value) && !valuesToToggleSelect.includes(value),
                ) ?? [],
            );
        },
        [
            valueToTreeNodeMap,
            selectedValues,
            updateSelectedValues,
            explicitlyUnselectChildren,
            explicitlyUnselectParent,
        ],
    );

    const toggleSelected = useCallback(
        (node: TreeNode) => {
            const isSelected = getIsSelected(node);

            if (isSelected) {
                unselect(node);
            } else {
                select(node);
            }
        },
        [getIsSelected, select, unselect],
    );

    // Children
    const getIsChildrenLoading = useCallback(
        (node: TreeNode) => {
            return !!(
                loadedValues.includes(node.value) &&
                node.hasAsyncChildren &&
                (!node.children?.length || (node.totalChildren && node.children?.length !== node.totalChildren))
            );
        },
        [loadedValues],
    );

    return (
        <TreeViewContext.Provider
            value={{
                nodes: preprocessedNodes,
                getHasParentNode,
                getIsExpandable,
                getIsExpanded,
                expand,
                collapse,
                toggleExpanded,
                getIsSelectable,
                getIsSelected,
                getIsParentSelected,
                getHasSelectedChildren,
                select,
                unselect,
                toggleSelected,
                explicitlySelectChildren,
                explicitlyUnselectChildren,
                explicitlySelectParent,
                explicitlyUnselectParent,
                renderNodeLabel,
                getIsChildrenLoading,
            }}
        >
            {children}
        </TreeViewContext.Provider>
    );
}
