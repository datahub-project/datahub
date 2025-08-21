import React from 'react';

import { DropdownProps } from '@components/components/Dropdown/types';
import { IconNames } from '@components/components/Icon';

export interface BaseItemType {
    type: string;
    key: string;
}

export interface MenuItemType extends BaseItemType {
    type: 'item';
    title: string;
    icon?: IconNames;
    description?: string;
    tooltip?: string;
    disabled?: boolean;
    danger?: boolean;
    children?: ItemType[];

    onClick?: () => void;
    render?: (item: MenuItemType) => React.ReactNode;
}

export interface GroupItemType extends BaseItemType {
    type: 'group';
    title: string;
    children?: ItemType[];
    render?: (item: GroupItemType) => React.ReactNode;
}

export interface DividerType extends BaseItemType {
    type: 'divider';
}

export type ItemType = MenuItemType | GroupItemType | DividerType;

export type MenuProps = Omit<DropdownProps, 'menu'> & {
    items?: ItemType[];
};

export interface MenuItemRendererProps {
    item: MenuItemType;
}

export interface GroupItemRendererProps {
    item: GroupItemType;
}
