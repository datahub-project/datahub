/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export interface NavMenuItem {
    icon?: React.FunctionComponent<React.SVGProps<SVGSVGElement> & { title?: string }>;
    title: string;
    showNewTag?: boolean;
    description: string;
    link?: string | null;
    subMenu?: NavSubMenu;
    isHidden?: boolean;
    target?: string;
    rel?: string;
    onClick?: () => void;
}

export interface NavSubMenuItem {
    title: string;
    showNewTag?: boolean;
    description: string;
    link?: string | null;
    isHidden?: boolean;
    target?: string;
    rel?: string;
    onClick?: () => void;
}

export interface NavSubMenu {
    isOpen: boolean;
    open: () => void;
    close: () => void;
    items: NavSubMenuItem[];
}
