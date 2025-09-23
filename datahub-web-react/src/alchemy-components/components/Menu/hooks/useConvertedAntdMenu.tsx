import { MenuProps } from 'antd';
import { ItemType as AntdItemType } from 'antd/lib/menu/hooks/useItems';
import React, { useMemo } from 'react';

import { RESET_DROPDOWN_MENU_STYLES_CLASSNAME } from '@components/components/Dropdown/constants';
import GroupItemRenderer from '@components/components/Menu/components/GroupItemRenderer';
import MenuItemRenderer from '@components/components/Menu/components/MenuItemRenderer';
import { ItemType } from '@components/components/Menu/types';

function convertItemsToAntdMenu(items: ItemType[]): MenuProps | undefined {
    const traverse = (item: ItemType): AntdItemType => {
        switch (item.type) {
            case 'item':
                return {
                    key: item.key,
                    label: item.render ? item.render(item) : <MenuItemRenderer item={item} />,
                    onClick: item.onClick,
                    disabled: item.disabled,
                    ...((item as any)['data-testid'] && { 'data-testid': (item as any)['data-testid'] }), // Pass through data-testid

                    ...(item?.children
                        ? {
                              children: item.children.map(traverse),
                              expandIcon: <></>, // hide the default expand icon
                              popupClassName: RESET_DROPDOWN_MENU_STYLES_CLASSNAME, // reset styles of submenu
                          }
                        : {}),
                };
            case 'group':
                return {
                    key: item.key,
                    type: 'group',
                    label: item.render ? item.render(item) : <GroupItemRenderer item={item} />,
                    ...(item?.children
                        ? {
                              children: item.children.map(traverse),
                          }
                        : {}),
                };
            case 'divider':
                return {
                    key: item.key,
                    type: 'divider',
                };
            default:
                return null;
        }
    };

    return { items: items.map(traverse) };
}

export default function useConvertedAntdMenu(items: ItemType[] | undefined) {
    return useMemo(() => convertItemsToAntdMenu(items ?? []), [items]);
}
