import { useMemo } from 'react';

import { ItemType, MenuItemType } from '@components/components/Menu/types';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
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
    const { availableProperties } = useAssetPropertiesContext();

    const menuItems: ItemType[] = useMemo(() => {
        // TODO: add structured properties
        const items = availableProperties
            .map((assetProperty) => assetPropertyToMenuItems(assetProperty, onClick))
            .sort((a, b) => a.title.localeCompare(b.title));

        return items;
    }, [onClick, availableProperties]);

    return menuItems;
}
