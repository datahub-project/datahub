import React from 'react';

import TreeNodesRenderer from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeNodesRenderer';
import TreeViewSkeleton from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeViewSkeleton';
import { TreeViewContextProvider } from '@app/homeV3/modules/hierarchyViewModule/treeView/context';
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
