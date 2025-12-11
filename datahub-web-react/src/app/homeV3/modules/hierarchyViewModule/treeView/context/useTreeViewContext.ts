/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext } from 'react';

import TreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/TreeViewContext';
import { TreeViewContextType } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

export default function useTreeViewContext() {
    return useContext<TreeViewContextType>(TreeViewContext);
}
