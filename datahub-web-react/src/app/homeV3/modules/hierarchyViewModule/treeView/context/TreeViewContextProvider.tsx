import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { DEFAULT_LOAD_BATCH_SIZE } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';
import TreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/TreeViewContext';
import { TreeNode, TreeViewContextProviderProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    addParentValueToTreeNodes,
    flattenTreeNodes,
    getAllParentValues,
    getAllValues,
    getValueToTreeNodeMapping,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

export default function TreeViewContextProvider({
    children,
    nodes,
    selectedValues,
    expandedValues,
    updateExpandedValues,
    onExpand,
    shouldExpandSingleRootNode,
    selectable,
    updateSelectedValues,
    expandParentNodesOfInitialSelectedValues,
    loadingTriggerType = 'button',
    rootNodesTotal: rootNodesTotalProperty,
    loadRootNodes,
    rootNodesLoading,
    loadChildren: loadAsyncChildren,
    renderNodeLabel,
    explicitlySelectChildren,
    explicitlyUnselectChildren,
    explicitlySelectParent,
    explicitlyUnselectParent,
    enableIntermediateSelectState,
    loadBatchSize = DEFAULT_LOAD_BATCH_SIZE,
}: React.PropsWithChildren<TreeViewContextProviderProps>) {
    const [internalExpandedValues, setInternalExpandedValues] = useState<string[]>(expandedValues ?? []);
    const [isExpandedValuesInitialized, setIsExpandedValuesInitialized] = useState<boolean>(false);

    const [loadedValues, setLoadedValues] = useState<string[]>([]);

    const preprocessedNodes = useMemo(() => {
        return addParentValueToTreeNodes(nodes);
    }, [nodes]);

    const flatTreeNodes = useMemo(() => flattenTreeNodes(preprocessedNodes), [preprocessedNodes]);
    const valueToTreeNodeMap = useMemo(() => getValueToTreeNodeMapping(flatTreeNodes), [flatTreeNodes]);

    const loadChildren = useCallback(
        (node: TreeNode) => {
            if (node.hasAsyncChildren) {
                loadAsyncChildren?.(node);
            }
        },
        [loadAsyncChildren],
    );

    // Initialize expanded values by initial selected values when `expandParentNodesOfInitialSelectedValues` is enabled
    useEffect(() => {
        if (!isExpandedValuesInitialized && expandParentNodesOfInitialSelectedValues) {
            const parentValues = (selectedValues ?? [])
                .map((value) => valueToTreeNodeMap[value]?.parentValue)
                .filter((parentValue): parentValue is string => !!parentValue);
            setInternalExpandedValues(parentValues);
            setIsExpandedValuesInitialized(true);
        }
    }, [selectedValues, isExpandedValuesInitialized, valueToTreeNodeMap, expandParentNodesOfInitialSelectedValues]);

    const initialChildrenLoad = useCallback(
        (node: TreeNode) => {
            if (!loadedValues.includes(node.value)) {
                setLoadedValues((prev) => [...prev, node.value]);
                loadChildren(node);
            }
        },
        [loadedValues, loadChildren],
    );

    const getHasParentNode = useCallback((node: TreeNode) => !!node.parentValue, []);

    const getIsRootNode = useCallback((node: TreeNode) => !getHasParentNode(node), [getHasParentNode]);

    const getRootNodes = useCallback(() => flatTreeNodes.filter((node) => !node.parentValue), [flatTreeNodes]);

    const getAllSiblings = useCallback(
        (node: TreeNode) => {
            const hasParentNode = getHasParentNode(node);

            if (hasParentNode) {
                return node.parentValue ? valueToTreeNodeMap[node.parentValue].children : [];
            }

            return getRootNodes();
        },
        [getHasParentNode, getRootNodes, valueToTreeNodeMap],
    );

    const rootNodesLength = useMemo(() => nodes.length, [nodes]);
    const rootNodesTotal = useMemo(
        () => rootNodesTotalProperty ?? rootNodesLength,
        [rootNodesTotalProperty, rootNodesLength],
    );

    const getChildrenLength = useCallback((node: TreeNode) => node.children?.length ?? 0, []);
    const getChildrenTotal = useCallback(
        (node: TreeNode) => node.totalChildren ?? getChildrenLength(node),
        [getChildrenLength],
    );

    // Expanding

    const getIsExpandable = useCallback((node: TreeNode) => !!node.children?.length || !!node.hasAsyncChildren, []);

    const getIsExpanded = useCallback(
        (node: TreeNode) => {
            const valuesToCheck = getAllValues([node]);
            return valuesToCheck.some((value) => internalExpandedValues.includes(value));
        },
        [internalExpandedValues],
    );

    const hasAnyExpanded = useMemo(() => internalExpandedValues.length > 0, [internalExpandedValues.length]);

    const expand = useCallback(
        (node: TreeNode) => {
            initialChildrenLoad(node); // try to load initial children on expand
            const parentValues = getAllParentValues(node, valueToTreeNodeMap);
            const newExpandedValues = [...internalExpandedValues, ...parentValues, node.value];
            setInternalExpandedValues(newExpandedValues);
            updateExpandedValues?.(newExpandedValues);
            onExpand?.(node);
        },
        [initialChildrenLoad, updateExpandedValues, onExpand, valueToTreeNodeMap, internalExpandedValues],
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

    const getHasAnyExpandableSiblings = useCallback(
        (node: TreeNode) => {
            const allSiblingNodes = getAllSiblings(node);
            return !!allSiblingNodes?.some((siblingNode) => getIsExpandable(siblingNode));
        },
        [getAllSiblings, getIsExpandable],
    );

    const [isInitialAutoExpandingDone, setIsInitialAutoExpandingDone] = useState<boolean>(false);

    useEffect(() => {
        if (!isInitialAutoExpandingDone && preprocessedNodes.length > 0) {
            if (shouldExpandSingleRootNode && preprocessedNodes.length === 1) {
                expand(preprocessedNodes[0]);
            }

            setIsInitialAutoExpandingDone(true);
        }
    }, [shouldExpandSingleRootNode, isInitialAutoExpandingDone, preprocessedNodes, expand]);

    // Sync internal expanded values
    useEffect(() => {
        if (expandedValues !== undefined) setInternalExpandedValues(expandedValues);
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

            const newSelectedValues =
                selectedValues?.filter(
                    (value) => !parentValues.includes(value) && !valuesToToggleSelect.includes(value),
                ) ?? [];
            updateSelectedValues?.(newSelectedValues);
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

    // Loading of children
    const getIsChildrenLoading = useCallback((node: TreeNode) => !!node.isChildrenLoading, []);

    return (
        <TreeViewContext.Provider
            value={{
                nodes: preprocessedNodes,

                // Node utils
                getHasParentNode,
                getIsRootNode,
                rootNodesLength,
                rootNodesTotal,
                getChildrenLength,
                getChildrenTotal,

                // Expanding
                getIsExpandable,
                getIsExpanded,
                hasAnyExpanded,
                expand,
                collapse,
                toggleExpanded,
                getHasAnyExpandableSiblings,

                // Selecting
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
                enableIntermediateSelectState,

                loadingTriggerType,
                loadRootNodes,
                rootNodesLoading,
                // Async loading of children
                getIsChildrenLoading,
                loadChildren,
                loadBatchSize,

                renderNodeLabel,
            }}
        >
            {children}
        </TreeViewContext.Provider>
    );
}
