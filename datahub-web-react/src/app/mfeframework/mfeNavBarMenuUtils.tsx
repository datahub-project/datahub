import {
    NavBarMenuDropdownItemElement,
    NavBarMenuItemTypes,
    NavBarMenuLinkItem,
} from '@app/homeV2/layout/navBarRedesign/types';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { MFESchema } from '@app/mfeframework/mfeConfigLoader';

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
            icon: getLazyIcon(mfe.navIcon),
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
            icon: getLazyIcon(mfe.navIcon),
            link: `/mfe${mfe.path}`,
            onClick: () => {
                console.log(`[MFE Nav] Clicked MFE nav item: ${mfe.label}, path: ${mfe.path}`);
            },
        }));
}
