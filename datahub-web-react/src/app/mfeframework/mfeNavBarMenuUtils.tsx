import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import React from 'react';

import {
    NavBarMenuDropdownItemElement,
    NavBarMenuItemTypes,
    NavBarMenuLinkItem,
} from '@app/homeV2/layout/navBarRedesign/types';
import { MFESchema } from '@app/mfeframework/mfeConfigLoader';

// TODO: Replace with React.lazy icon resolution so MFE nav icons can use any Phosphor icon.
// For now all MFE nav items show the default AppWindow icon.
function getPhosphorIconElement(): JSX.Element {
    return <AppWindow />;
}

/**
 * Returns menu items for MFEs when subNavigationMode is FALSE.
 * Only MFEs with showInNav=true are included.
 */
export function getMfeMenuItems(mfeConfig: MFESchema): NavBarMenuLinkItem[] {
    if (!mfeConfig?.microFrontends) return [];

    return mfeConfig.microFrontends
        .filter((mfe) => mfe.flags?.showInNav)
        .map((mfe) => ({
            type: NavBarMenuItemTypes.Item,
            title: mfe.label,
            key: mfe.id,
            icon: getPhosphorIconElement(),
            link: `/mfe${mfe.path}`,
            onClick: () => {
                console.log(`[MFE Nav] Clicked MFE nav item: ${mfe.label}, path: ${mfe.path}`);
            },
        }));
}

/**
 * Returns dropdown menu items for MFEs when subNavigationMode is TRUE.
 * Only MFEs with showInNav=true are included.
 */
export function getMfeMenuDropdownItems(mfeConfig: MFESchema): NavBarMenuDropdownItemElement[] {
    if (!mfeConfig?.microFrontends) return [];

    return mfeConfig.microFrontends
        .filter((mfe) => mfe.flags?.showInNav)
        .map((mfe) => ({
            type: NavBarMenuItemTypes.DropdownElement,
            title: mfe.label,
            key: mfe.id,
            icon: getPhosphorIconElement(),
            link: `/mfe${mfe.path}`,
            onClick: () => {
                console.log(`[MFE Nav] Clicked MFE nav item: ${mfe.label}, path: ${mfe.path}`);
            },
        }));
}
