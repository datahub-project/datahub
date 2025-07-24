import { Entity } from '@types';

// TODO: make it generic before moving TreeView component into the components library
export type TreeNode = {
    value: string;
    label: string;

    selectable?: boolean;

    parentValue?: string;
    children?: TreeNode[];
    hasAsyncChildren?: boolean;
    totalChildren?: number;
    isChildrenLoading?: boolean;
    entity: Entity;
};

export interface ValueToTreeNodeMap {
    [key: string]: TreeNode;
}

export interface TreeNodeProps {
    node: TreeNode;
    depth: number;
}

export interface TreeViewContextType {
    nodes: TreeNode[];

    getHasParentNode: (node: TreeNode) => boolean;
    getIsRootNode: (node: TreeNode) => boolean;

    // Expand
    getIsExpandable: (node: TreeNode) => boolean;
    getIsExpanded: (node: TreeNode) => boolean;
    expand: (node: TreeNode) => void;
    collapse: (node: TreeNode) => void;
    toggleExpanded: (node: TreeNode) => void;
    getHasAnyExpandableSiblings: (node: TreeNode) => boolean;

    // Select
    getIsSelectable: (node: TreeNode) => boolean;
    getIsSelected: (node: TreeNode) => boolean;
    getIsParentSelected: (node: TreeNode) => boolean;
    getHasSelectedChildren: (node: TreeNode) => boolean;
    select: (node: TreeNode) => void;
    unselect: (node: TreeNode) => void;
    toggleSelected: (node: TreeNode) => void;
    expandInitialSelectedNodes?: boolean;
    explicitlySelectChildren?: boolean;
    explicitlyUnselectChildren?: boolean;
    explicitlySelectParent?: boolean;
    explicitlyUnselectParent?: boolean;
    enableIntermediateSelectState?: boolean;

    // Async loading of children
    // -------------------------------------------------
    getIsChildrenLoading: (node: TreeNode) => boolean;
    getNumberOfNotLoadedChildren: (node: TreeNode) => number;
    loadChildren: (node: TreeNode) => void;
    // Max number of children to load per each loadChildren call
    numberOfChildrenToLoad: number;

    // Optional custom node label renderer
    renderNodeLabel?: (props: TreeNodeProps) => React.ReactNode;
}

export interface TreeViewContextProviderProps {
    // Array of nodes to show in the tree view
    nodes: TreeNode[];

    // EXPANDING/COLLAPSING
    // List of expanded values (controlled state)
    expandedValues?: string[];
    // Called when expanding state changed (`values` is the full list of expanded values)
    updateExpandedValues?: (values: string[]) => void;

    // SELECTION
    // If enabled it shows checkboxes next to nodes and enables selecting
    selectable?: boolean;
    // List of selected values (controlled state)
    selectedValues?: string[];
    // Called when selection state changed (`values` is the full list of selected values)
    updateSelectedValues?: (values: string[]) => void;
    // If enabled all
    expandInitialSelectedNodes?: boolean;
    // If enabled it prevents selecting of all children if parent was selected
    explicitlySelectChildren?: boolean;
    // If enabled it prevents unselecting of children if parent was unselected
    explicitlyUnselectChildren?: boolean;
    // If enabled it prevents selecting of parent if all its children were selected
    explicitlySelectParent?: boolean;
    // If enabled it prevents unselecting of parent if any its children were unselected
    explicitlyUnselectParent?: boolean;
    // If enabled it shows intermediate state of checkbox when the current node is not selected but it has selected nested nodes
    enableIntermediateSelectState?: boolean;

    // Optional custom renderer of nodes
    renderNodeLabel?: (props: TreeNodeProps) => React.ReactNode;

    // Async
    // Callback to load children of a specific node
    loadChildren?: (node: TreeNode) => void;
    numberOfChildrenToLoad?: number;
}
