import React from 'react';

export enum NavBarMenuItemTypes {
    Group = 'SECTION',
    Item = 'ITEM',
    Dropdown = 'DROPDOWN',
    DropdownElement = 'DROPDOWN_ELEMENT',
    Custom = 'CUSTOM',
}

export interface NavBarMenuBaseElement {
    type: NavBarMenuItemTypes;
    key: string;
    description?: string;
    link?: string;
    // Additional links to detect selected menu item (for subroutes)
    additionalLinksForPathMatching?: string[];
    //
    onlyExactPathMapping?: boolean;
    isExternalLink?: boolean;
    isHidden?: boolean;
    onClick?: () => void;
    disabled?: boolean;
    href?: string;
    dataTestId?: string;
}

export type Badge = {
    count: number;
    show?: boolean;
};

export interface NavBarMenuBaseItem extends NavBarMenuBaseElement {
    icon: React.ReactNode;
    selectedIcon?: React.ReactNode;
    title: string;
    badge?: Badge;
}

export type NavBarMenuLinkItem = NavBarMenuBaseItem & {
    type: NavBarMenuItemTypes.Item;
    title: string;
};

export type NavBarMenuDropdownItem = NavBarMenuBaseItem & {
    type: NavBarMenuItemTypes.Dropdown;
    items?: NavBarMenuDropdownItemElement[];
};

export type NavBarMenuDropdownItemElement = NavBarMenuBaseElement & {
    type: NavBarMenuItemTypes.DropdownElement;
    title: string;
};

export type NavBarMenuGroup = NavBarMenuBaseElement & {
    title?: string;
    type: NavBarMenuItemTypes.Group;
    items?: Array<NavBarMenuLinkItem | NavBarMenuDropdownItem>;
};

export type NavBarCustomElement = NavBarMenuBaseElement & {
    type: NavBarMenuItemTypes.Custom;
    render: () => React.ReactNode;
};

export type AnyMenuItem = NavBarMenuGroup | NavBarMenuLinkItem | NavBarMenuDropdownItem | NavBarCustomElement;

export interface NavBarMenuItems {
    items: AnyMenuItem[];
}
