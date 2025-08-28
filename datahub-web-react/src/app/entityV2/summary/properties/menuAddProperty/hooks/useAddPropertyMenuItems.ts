import { useMemo } from 'react';

import { ItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { assetPropertyToMenuItem } from '@app/entityV2/summary/properties/utils';

export default function useAddPropertyMenuItems(onClick: (property: AssetProperty) => void): ItemType[] {
    const basicAssetProperties = useBasicAssetProperties();
    const structuredPropertiesMenuItems = useStructuredPropertiesMenuItems(onClick);

    const menuItems: ItemType[] = useMemo(() => {
        const items = basicAssetProperties.map((assetProperty) => assetPropertyToMenuItem(assetProperty, onClick));

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
