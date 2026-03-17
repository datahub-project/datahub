import { useMemo } from 'react';

import { ItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useStructuredPropertiesMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useStructuredPropertiesMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { assetPropertyToMenuItem } from '@app/entityV2/summary/properties/utils';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

export default function useAddPropertyMenuItems(onClick: (property: AssetProperty) => void): ItemType[] {
    const basicAssetProperties = useBasicAssetProperties();
    const { summaryElements } = usePageTemplateContext();
    const structuredPropertiesMenuItems = useStructuredPropertiesMenuItems(onClick);

    const menuItems: ItemType[] = useMemo(() => {
        const visiblePropertyTypes = new Set(summaryElements?.map((el) => el.type) ?? []);
        const visibleStructuredPropertyUrns = new Set(
            summaryElements
                ?.filter((el) => el.structuredProperty)
                .map((el) => el.structuredProperty?.urn)
                .filter((urn): urn is string => urn !== undefined) ?? [],
        );

        const items = basicAssetProperties
            .filter((assetProperty) => {
                if (visiblePropertyTypes.has(assetProperty.type)) {
                    return false;
                }
                if (assetProperty.structuredProperty) {
                    return !visibleStructuredPropertyUrns.has(assetProperty.structuredProperty.urn);
                }
                return true;
            })
            .map((assetProperty) => assetPropertyToMenuItem(assetProperty, onClick));

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
    }, [onClick, basicAssetProperties, structuredPropertiesMenuItems, summaryElements]);

    return menuItems;
}
