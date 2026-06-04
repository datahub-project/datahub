import type { EntityRegistry } from '@src/entityRegistryContext';
import type { BrowseResultGroupV2 } from '@types';

import { BrowseSortOrder } from '@app/searchV2/sidebar/BrowseSortContext';

export function getBrowseResultGroupLabel(group: BrowseResultGroupV2, entityRegistry: EntityRegistry): string {
    return group.entity ? entityRegistry.getDisplayName(group.entity.type, group.entity) : group.name;
}

export function sortBrowseResultGroups(
    groups: BrowseResultGroupV2[],
    sortOrder: BrowseSortOrder,
    entityRegistry: EntityRegistry,
) {
    if (sortOrder === BrowseSortOrder.RECENTLY_USED) {
        return groups;
    }

    const direction = sortOrder === BrowseSortOrder.ALPHABETICAL_DESC ? -1 : 1;

    return [...groups].sort((firstGroup, secondGroup) => {
        return (
            getBrowseResultGroupLabel(firstGroup, entityRegistry).localeCompare(
                getBrowseResultGroupLabel(secondGroup, entityRegistry),
            ) * direction
        );
    });
}
