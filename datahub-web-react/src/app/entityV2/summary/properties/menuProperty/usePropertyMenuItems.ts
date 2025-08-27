import { useCallback, useMemo } from 'react';

import { ItemType } from '@components/components/Menu/types';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

export default function usePropertyMenuItems(position: number): ItemType[] {
    const { remove, replace } = useAssetPropertiesContext();

    const onRemove = useCallback(() => remove(position), [remove, position]);
    const onReplace = useCallback((newProperty: AssetProperty) => replace(newProperty, position), [replace, position]);

    const addPropertyMenuItems = useAddPropertyMenuItems(onReplace);

    const menuItems: ItemType[] = useMemo(() => {
        const items: ItemType[] = [
            {
                type: 'item',
                key: 'replace',
                title: 'Replace Property',
                children: addPropertyMenuItems,
            },
            {
                type: 'item',
                key: 'remove',
                title: 'Remove',
                onClick: onRemove,
                danger: true,
            },
        ];

        return items;
    }, [addPropertyMenuItems, onRemove]);

    return menuItems;
}
