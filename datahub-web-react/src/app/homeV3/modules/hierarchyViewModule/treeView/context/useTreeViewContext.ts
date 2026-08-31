import { useContext } from 'react';

import TreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/TreeViewContext';
import { TreeViewContextType } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

export default function useTreeViewContext() {
    return useContext<TreeViewContextType>(TreeViewContext);
}
