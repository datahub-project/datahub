import { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

import { SummaryElementType } from '@types';

export default function usePropertyMenuItems(position: number, elementType: SummaryElementType): ItemType[] {
    const { t } = useTranslation('entity.profile.summary');
    const { t: ta } = useTranslation('common.actions');
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
                key: 'remove',
                title: ta('remove'),
                onClick: onRemove,
                danger: true,
            },
        ];

        if (addPropertyMenuItems.length > 0) {
            // add to the beginning
            items.unshift({
                type: 'item',
                key: 'replace',
                title: t('menu.replaceProperty'),
                children: addPropertyMenuItems,
            });
        }

        return items;
    }, [t, ta, addPropertyMenuItems, onRemove]);

    return menuItems;
}
