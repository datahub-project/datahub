import { useMemo } from 'react';

import { ItemType, MenuItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

function assetPropertyToMenuItems(
    assetProperty: AssetProperty,
    onMenuItemClick: (assetProperty: AssetProperty) => void,
): MenuItemType {
    return {
        type: 'item',
        key: assetProperty.key ?? assetProperty.type,
        title: assetProperty.name,
        icon: assetProperty.icon,
        onClick: () => onMenuItemClick(assetProperty),
    };
}

export default function useAddPropertyMenuItems(onClick: (property: AssetProperty) => void): ItemType[] {
    const basicAssetProperties = useBasicAssetProperties();
    const structuredPropertiesMenuItems = useStructuredPropertiesMenuItems(onClick);

    const menuItems: ItemType[] = useMemo(() => {
        const items = basicAssetProperties.map((assetProperty) => assetPropertyToMenuItems(assetProperty, onClick));

        if (structuredPropertiesMenuItems.length > 0) {
            items.push({
                type: 'item',
                key: 'structuredProperties',
                title: 'Properties',
                icon: 'ListDashes',
                children: structuredPropertiesMenuItems,
            });
        }

        return sortMenuItems(items);
    }, [onClick, basicAssetProperties, structuredPropertiesMenuItems]);

    return menuItems;
}
