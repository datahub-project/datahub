/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EntityRegistry } from '@src/entityRegistryContext';

import { Entity } from '@types';

export function getAutoCompleteEntityText(displayName: string, query: string) {
    const isPrefixMatch = displayName.toLowerCase().startsWith(query.toLowerCase());
    const matchedText = isPrefixMatch ? displayName.substring(0, query.length) : '';
    const unmatchedText = isPrefixMatch ? displayName.substring(query.length, displayName.length) : displayName;

    return { matchedText, unmatchedText };
}

export function getShouldDisplayTooltip(entity: Entity, entityRegistry: EntityRegistry) {
    const hasDatasetStats = (entity as any).lastProfile?.length > 0 || (entity as any).statsSummary?.length > 0;
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentContainers = genericEntityProps?.parentContainers?.containers;
    const hasMoreThanTwoContainers = parentContainers && parentContainers.length > 2;

    return hasDatasetStats || !!hasMoreThanTwoContainers;
}
