/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { DEFAULT_LOAD_BATCH_SIZE } from '@app/homeV3/modules/hierarchyViewModule/treeView/constants';
import { TreeViewContextType } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const DEFAULT_TREE_VIEW_CONTEXT: TreeViewContextType = {
    nodes: [],

    getHasParentNode: () => false,
    getIsRootNode: () => false,
    rootNodesLength: 0,
    rootNodesTotal: 0,
    getChildrenLength: () => 0,
    getChildrenTotal: () => 0,

    getIsExpandable: () => false,
    getIsExpanded: () => false,
    hasAnyExpanded: false,
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
    loadChildren: () => {},
    loadBatchSize: DEFAULT_LOAD_BATCH_SIZE,

    explicitlySelectChildren: false,
    explicitlyUnselectChildren: false,
    explicitlySelectParent: false,
    explicitlyUnselectParent: false,
};

const TreeViewContext = React.createContext<TreeViewContextType>(DEFAULT_TREE_VIEW_CONTEXT);

export default TreeViewContext;
