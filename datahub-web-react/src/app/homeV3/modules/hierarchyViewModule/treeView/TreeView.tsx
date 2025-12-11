/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import TreeNodesRenderer from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeNodesRenderer';
import TreeViewSkeleton from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeViewSkeleton';
import TreeViewContextProvider from '@app/homeV3/modules/hierarchyViewModule/treeView/context/TreeViewContextProvider';
import { TreeViewContextProviderProps } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface Props extends TreeViewContextProviderProps {
    loading?: boolean;
}

export default function TreeView({ loading, ...props }: Props) {
    if (loading) {
        return <TreeViewSkeleton />;
    }

    return (
        <TreeViewContextProvider {...props}>
            <TreeNodesRenderer />
        </TreeViewContextProvider>
    );
}
