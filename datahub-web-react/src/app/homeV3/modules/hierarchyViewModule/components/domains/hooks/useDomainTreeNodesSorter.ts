/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';

import { sortDomainTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

export default function useDomainTreeNodesSorter() {
    const entityRegistry = useEntityRegistryV2();

    return useCallback((nodes: TreeNode[]) => sortDomainTreeNodes(nodes, entityRegistry), [entityRegistry]);
}
