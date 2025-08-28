import { MenuItemType } from '@components/components/Menu/types';

import { AssetProperty } from '@app/entityV2/summary/properties/types';

export function assetPropertyToMenuItem(
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
