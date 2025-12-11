/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Entity } from '@types';

export function convertEntityToTreeNode(entity: Entity, parentValue?: string): TreeNode {
    return {
        // add parent value to avoid duplicated values as the same entity could be attached to different assets
        value: `${parentValue ?? ''} / ${entity.urn}`,
        label: entity.urn,
        entity,
    };
}
