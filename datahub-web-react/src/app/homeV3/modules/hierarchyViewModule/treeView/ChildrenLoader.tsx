/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
