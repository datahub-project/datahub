import { ItemType } from '@components/components/Menu/types';

import { AssetProperty } from '@app/entityV2/summary/properties/types';

import { SummaryElementType } from '@types';

export function filterCurrentItemInReplaceMenu(menuItems: ItemType[], property: AssetProperty) {
    const excludeKey =
        property.type === SummaryElementType.StructuredProperty ? property.structuredProperty?.urn : property.type;

    return menuItems.map((item) => {
        if (item.key !== 'replace') {
            return item;
        }

        if (!('children' in item) || !item.children) {
            return item;
        }

        if (property.type === SummaryElementType.StructuredProperty) {
            return {
                ...item,
                children: item.children.map((child) =>
                    child.key === 'structuredProperties' && 'children' in child
                        ? {
                              ...child,
                              children: child.children?.filter((c) => c.key !== excludeKey),
                          }
                        : child,
                ),
            };
        }

        return {
            ...item,
            children: item.children.filter((child) => child.key !== excludeKey),
        };
    });
}
