import React from 'react';
import { Dropdown, MenuItemProps } from 'antd';
import { useHistory } from 'react-router';
import NavBarMenuItem from './NavBarMenuItem';
import { NavBarMenuDropdownItem } from './types';

type Props = {
    item: NavBarMenuDropdownItem;
} & MenuItemProps;

export default function NavBarMenuItemDropdown({ item, ...props }: Props) {
    const history = useHistory();

    const menu = item.items
        ?.filter((subItem) => !subItem.isHidden)
        .map((subItem) => ({
            title: subItem.description,
            label: subItem.title,
            key: subItem.key,
        }));

    const onItemClick = (key) => {
        const clickedItem = item.items?.filter((dropdownItem) => dropdownItem.key === key)?.[0];
        if (!clickedItem) return null;

        if (clickedItem.onClick) return clickedItem.onClick();

        if (clickedItem.link && clickedItem.isExternalLink)
            return window.open(clickedItem.link, '_blank', 'noopener,noreferrer');
        if (clickedItem.link && !clickedItem.isExternalLink) return history.push(clickedItem.link);

        return null;
    };

    return (
        <Dropdown menu={{ items: menu, onClick: (attrs) => onItemClick(attrs.key) }}>
            <NavBarMenuItem item={item} {...props} />
        </Dropdown>
    );
}
