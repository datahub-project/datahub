import { useCallback, useMemo } from 'react';

import { ItemType } from '@components/components/Menu/types';

import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

import { SummaryElementType } from '@types';

export default function usePropertyMenuItems(position: number, elementType: SummaryElementType): ItemType[] {
    const { removeSummaryElement, replaceSummaryElement } = usePageTemplateContext();

    const onRemove = useCallback(
        () => removeSummaryElement(position, elementType),
        [removeSummaryElement, position, elementType],
    );
    const onReplace = useCallback(
        (newElement: AssetProperty) =>
            replaceSummaryElement({
                elementType: newElement.type,
                structuredProperty: newElement.structuredProperty,
                position,
                currentElementType: elementType,
            }),
        [replaceSummaryElement, position, elementType],
    );

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
