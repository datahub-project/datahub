import { useEffect, useState } from 'react';

import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface Props {
    node: TreeNode;
}

export default function ChildrenLoader({ node }: Props) {
    const { loadChildren } = useTreeViewContext();

    const [isInitialized, setIsInitialized] = useState<boolean>(false);

    useEffect(() => {
        if (!isInitialized && node.hasAsyncChildren) {
            loadChildren?.(node);
            setIsInitialized(true);
        }
    }, [node, isInitialized, loadChildren]);
    return null;
}
