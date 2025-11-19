import * as PhosphorIcons from '@phosphor-icons/react';
import { AppWindow } from '@phosphor-icons/react';
import React from 'react';

import {
    NavBarMenuDropdownItemElement,
    NavBarMenuItemTypes,
    NavBarMenuLinkItem,
} from '@app/homeV2/layout/navBarRedesign/types';
import { MFEConfig, MFEConfigEntry, MFESchema } from '@app/mfeframework/mfeConfigLoader';

/* Converts any phosphor icon string into a phosphor JSX.Element, if iconString is undefined or not a valid icon, default is <AppWindow/>.
  eg: Tag wil become <Tag /> */
function getPhosphorIconElement(iconString?: string): JSX.Element {
    const Component = iconString && PhosphorIcons[iconString];
    return Component ? <Component /> : <AppWindow />;
}

// Type guard to check if an entry is a valid MFEConfig.
function isValidMFEConfig(mfe: MFEConfigEntry): mfe is MFEConfig {
    return !('invalid' in mfe && mfe.invalid);
}

// Set to track which error messages have already been logged
const loggedInvalidMfeIssues = new Set<string>();

function logInvalidMfeIssues(mfe: MFEConfigEntry) {
    const id = 'id' in mfe && mfe.id ? mfe.id : 'unknown';
    if ('invalid' in mfe && mfe.invalid && Array.isArray((mfe as any).errorMessages)) {
        (mfe as any).errorMessages.forEach((msg: string) => {
            const uniqueKey = `${id}:${msg}`;
            if (!loggedInvalidMfeIssues.has(uniqueKey)) {
                console.error(`[MFE Nav] Invalid MFE config for id: ${id} - ${msg}`);
                loggedInvalidMfeIssues.add(uniqueKey);
            }
        });
    }
}

/**
 * Returns menu items for MFEs when subNavigationMode is FALSE.
 * Only valid MFEs (not marked as invalid) and with showInNav=true are included.
 * Logs console errors for invalid MFEs.
 */
export function getMfeMenuItems(mfeConfig: MFESchema): NavBarMenuLinkItem[] {
    if (!mfeConfig?.microFrontends) return [];

    mfeConfig.microFrontends.forEach((mfe) => {
        logInvalidMfeIssues(mfe);
    });

    return mfeConfig.microFrontends
        .filter(isValidMFEConfig)
        .filter((mfe) => mfe.flags?.showInNav)
        .map((mfe) => ({
            type: NavBarMenuItemTypes.Item,
            title: mfe.label,
            key: mfe.id,
            icon: getPhosphorIconElement(mfe.navIcon),
            link: `/mfe${mfe.path}`,
            onClick: () => {
                console.log(`[MFE Nav] Clicked MFE nav item: ${mfe.label}, path: ${mfe.path}`);
            },
        }));
}

/**
 * Returns dropdown menu items for MFEs when subNavigationMode is TRUE.
 * Only valid MFEs (not marked as invalid) and with showInNav=true are included.
 * Logs console errors for invalid MFEs.
 */
export function getMfeMenuDropdownItems(mfeConfig: MFESchema): NavBarMenuDropdownItemElement[] {
    if (!mfeConfig?.microFrontends) return [];

    mfeConfig.microFrontends.forEach((mfe) => {
        logInvalidMfeIssues(mfe);
    });

    return mfeConfig.microFrontends
        .filter(isValidMFEConfig)
        .filter((mfe) => mfe.flags?.showInNav)
        .map((mfe) => ({
            type: NavBarMenuItemTypes.DropdownElement,
            title: mfe.label,
            key: mfe.id,
            icon: getPhosphorIconElement(mfe.navIcon),
            link: `/mfe${mfe.path}`,
            onClick: () => {
                console.log(`[MFE Nav] Clicked MFE nav item: ${mfe.label}, path: ${mfe.path}`);
            },
        }));
}
