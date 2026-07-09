import React from 'react';
import { describe, expect, it } from 'vitest';

import { NavBarMenuItemTypes } from '@app/homeV2/layout/navBarRedesign/types';
import { MFESchema } from '@app/mfeframework/mfeConfigLoader';
import { getMfeMenuDropdownItems, getMfeMenuItems } from '@app/mfeframework/mfeNavBarMenuUtils';

describe('getMfeMenuItems and getMfeMenuDropdownItems', () => {
    it('returns only showInNav=true items with valid icon elements', () => {
        const mfeConfig: MFESchema = {
            topLevelMenuTitle: 'My Apps',
            subNavigationMode: false,
            microFrontends: [
                {
                    id: 'mfe-1',
                    label: 'MFE One',
                    path: '/mfe-one',
                    remoteEntry: 'https://mfe-one.com/remoteEntry.js',
                    module: 'mfeOneApplication/mount',
                    navIcon: 'Package',
                    flags: { enabled: true, showInNav: true },
                },
                {
                    id: 'mfe-2',
                    label: 'MFE Two',
                    path: '/mfe-two',
                    remoteEntry: 'https://mfe-two.com/remoteEntry.js',
                    module: 'mfeTwoApplication/mount',
                    navIcon: 'Globe',
                    flags: { enabled: true, showInNav: false },
                },
                {
                    id: 'mfe-3',
                    label: 'MFE Three',
                    path: '/mfe-three',
                    remoteEntry: 'https://mfe-three.com/remoteEntry.js',
                    module: 'mfeThreeApplication/mount',
                    navIcon: 'Globe',
                    flags: { enabled: true, showInNav: true },
                },
                {
                    id: 'mfe-4',
                    label: 'MFE Four',
                    path: '/mfe-four',
                    remoteEntry: 'https://mfe-four.com/remoteEntry.js',
                    module: 'mfeFourApplication/mount',
                    navIcon: 'UnknownIcon',
                    flags: { enabled: true, showInNav: true },
                },
            ],
        };

        const items = getMfeMenuItems(mfeConfig);

        expect(items).toHaveLength(3);

        expect(items[0].title).toBe('MFE One');
        expect(items[0].key).toBe('mfe-1');
        expect(items[0].link).toBe('/mfe/mfe-one');
        expect(React.isValidElement(items[0].icon)).toBe(true);

        expect(items[1].title).toBe('MFE Three');
        expect(React.isValidElement(items[1].icon)).toBe(true);

        // Unknown navIcon still produces a valid element (renders fallback via Suspense)
        expect(items[2].title).toBe('MFE Four');
        expect(React.isValidElement(items[2].icon)).toBe(true);

        items.forEach((item) => {
            expect(item.type).toBe(NavBarMenuItemTypes.Item);
            expect(typeof item.onClick).toBe('function');
        });
    });

    it('returns items of type DropdownElement when subNavigationMode is true', () => {
        const mfeConfig: MFESchema = {
            topLevelMenuTitle: 'My Apps',
            subNavigationMode: true,
            microFrontends: [
                {
                    id: 'mfe-1',
                    label: 'MFE One',
                    path: '/mfe-one',
                    remoteEntry: 'https://mfe-one.com/remoteEntry.js',
                    module: 'mfeOneApplication/mount',
                    navIcon: 'Package',
                    flags: { enabled: true, showInNav: true },
                },
                {
                    id: 'mfe-2',
                    label: 'MFE Two',
                    path: '/mfe-two',
                    remoteEntry: 'https://mfe-two.com/remoteEntry.js',
                    module: 'mfeTwoApplication/mount',
                    navIcon: 'Globe',
                    flags: { enabled: true, showInNav: true },
                },
            ],
        };
        const items = getMfeMenuDropdownItems(mfeConfig);
        expect(items).toHaveLength(2);
        items.forEach((item) => {
            expect(item.type).toBe(NavBarMenuItemTypes.DropdownElement);
            expect(React.isValidElement(item.icon)).toBe(true);
        });
    });

    it('returns empty array if no microFrontends (getMfeMenuItems)', () => {
        const mfeConfig: MFESchema = { topLevelMenuTitle: 'My Apps', microFrontends: [], subNavigationMode: false };
        expect(getMfeMenuItems(mfeConfig)).toEqual([]);
    });

    it('returns empty array if no microFrontends (getMfeMenuDropdownItems)', () => {
        const mfeConfig: MFESchema = { topLevelMenuTitle: 'My Apps', microFrontends: [], subNavigationMode: true };
        expect(getMfeMenuDropdownItems(mfeConfig)).toEqual([]);
    });
});
