/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { GenericEntityProperties } from '@app/entity/shared/types';

import { DataProduct, Entity, EntityType } from '@types';

type GetContextPathInput = Pick<
    GenericEntityProperties,
    'parent' | 'parentContainers' | 'parentDomains' | 'parentNodes' | 'domain'
>;

export function getParentEntities(entityData: GetContextPathInput | null, entityType?: EntityType): Entity[] {
    if (!entityData) return [];

    switch (entityType) {
        case EntityType.DataProduct: {
            const domain = (entityData as DataProduct).domain?.domain;
            return domain ? [domain, ...(domain.parentDomains?.domains || [])] : [];
        }

        case EntityType.GlossaryTerm:
        case EntityType.GlossaryNode:
            return entityData.parentNodes?.nodes || [];

        case EntityType.Domain:
            return entityData.parentDomains?.domains || [];

        default: {
            // generic fallback
            const containerPath =
                entityData.parentContainers?.containers ||
                entityData.parentDomains?.domains ||
                entityData.parentNodes?.nodes ||
                [];
            if (containerPath.length) return containerPath;

            if (entityData.parent) return [entityData.parent as Entity];

            return [];
        }
    }
}
