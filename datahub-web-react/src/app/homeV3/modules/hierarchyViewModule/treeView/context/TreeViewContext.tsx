import React from 'react';

import { DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';
import { TreeViewContextType } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const DEFAULT_TREE_VIEW_CONTEXT: TreeViewContextType = {
    nodes: [],

    getHasParentNode: () => false,
    getIsRootNode: () => false,

    getIsExpandable: () => false,
    getIsExpanded: () => false,
    expand: () => {},
    collapse: () => {},
    toggleExpanded: () => {},
    getHasAnyExpandableSiblings: () => false,

    getIsSelectable: () => false,
    getIsSelected: () => false,
    getIsParentSelected: () => false,
    getHasSelectedChildren: () => false,
    select: () => {},
    unselect: () => {},
    toggleSelected: () => {},

    getIsChildrenLoading: () => false,
    getNumberOfNotLoadedChildren: () => 0,
    loadChildren: () => {},
    numberOfChildrenToLoad: DEFAULT_NUMBER_OF_CHILDREN_TO_LOAD,

    explicitlySelectChildren: false,
    explicitlyUnselectChildren: false,
    explicitlySelectParent: false,
    explicitlyUnselectParent: false,
};

const TreeViewContext = React.createContext<TreeViewContextType>(DEFAULT_TREE_VIEW_CONTEXT);

export default TreeViewContext;
