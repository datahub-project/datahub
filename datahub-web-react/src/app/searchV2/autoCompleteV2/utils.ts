/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import EntityRegistry from '@src/app/entityV2/EntityRegistry';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { Entity } from '@src/types.generated';

export function getEntityDisplayType(entity: Entity, registry: EntityRegistry) {
    const properties = registry.getGenericEntityProperties(entity.type, entity);

    const subtype = registry.getFirstSubType(properties);
    const entityName = registry.getEntityName(entity.type);

    return subtype ? capitalizeFirstLetterOnly(subtype) : entityName;
}
