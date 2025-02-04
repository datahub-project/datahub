import React from 'react';
import { Menu, MenuProps } from 'antd';
import styled from 'styled-components';
import NavBarMenuItem from './NavBarMenuItem';
import NavBarMenuItemDropdown from './NavBarMenuItemDropdown';
import { AnyMenuItem, NavBarMenuItems, NavBarMenuItemTypes } from './types';
import NavBarMenuItemGroup from './NavBarMenuItemGroup';

const StyledMenu = styled(Menu)`
    && {
        background: none;
        padding: 0;
        margin: 0;
        border: 0;
        display: flex;
        flex-direction: column;
        height: 100%;
    }

    && .ant-menu-item-group {
        width: 100%;
    }
`;

type Props = {
    menu: NavBarMenuItems;
    selectedKey: string;
    isCollapsed: boolean;
    style?: any;
    iconSize?: number;
} & Omit<MenuProps, 'items'>;

export default function NavBarMenu({ menu, selectedKey, isCollapsed, iconSize, style }: Props) {
    const renderMenuItem = (item: AnyMenuItem) => {
        if (item.isHidden) return null;

        const isSelected = selectedKey === item.key;

        if (item.type === NavBarMenuItemTypes.Group && item.items?.filter((candidate) => !candidate.isHidden)?.length) {
            return (
                <NavBarMenuItemGroup title={!isCollapsed && item.title} key={item.key}>
                    {item.items?.map((subItem) => renderMenuItem(subItem))}
                </NavBarMenuItemGroup>
            );
        }

        if (item.type === NavBarMenuItemTypes.Item) {
            return (
                <NavBarMenuItem
                    item={item}
                    key={item.key}
                    isCollapsed={isCollapsed}
                    isSelected={isSelected}
                    iconSize={iconSize}
                />
            );
        }

        if (item.type === NavBarMenuItemTypes.Dropdown) {
            return (
                <NavBarMenuItemDropdown item={item} key={item.key} isCollapsed={isCollapsed} isSelected={isSelected} />
            );
        }

        if (item.type === NavBarMenuItemTypes.Custom) {
            return item.render();
        }

        return null;
    };

    return (
        <StyledMenu selectedKeys={selectedKey ? [selectedKey] : []} style={style} data-testid="nav-menu-links">
            {menu.items.map((item) => renderMenuItem(item))}
        </StyledMenu>
    );
}
