import { AppWindow, Archive } from '@phosphor-icons/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { NavBarMenuItemTypes } from '@app/homeV2/layout/navBarRedesign/types';
import { MFESchema } from '@app/mfeframework/mfeConfigLoader';
import { getMfeMenuDropdownItems, getMfeMenuItems } from '@app/mfeframework/mfeNavBarMenuUtils';

describe('getMfeMenuItems and getMfeMenuDropdownItems', () => {
    it('returns only valid menu items where flags.showInNav is true and maps navIcon to correct icon', () => {
        const mfeConfig: MFESchema = {
            subNavigationMode: false,
            microFrontends: [
                {
                    id: 'mfe-1',
                    label: 'MFE One',
                    path: '/mfe-one',
                    remoteEntry: 'https://mfe-one.com/remoteEntry.js',
                    module: 'mfeOneApplication/mount',
                    navIcon: 'Archive',
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
                    // navIcon not set, should default to AppWindow
                    navIcon: '',
                    flags: { enabled: true, showInNav: true },
                },
                {
                    id: 'mfe-4',
                    label: 'MFE Four',
                    path: '/mfe-four',
                    remoteEntry: 'https://mfe-four.com/remoteEntry.js',
                    module: 'mfeFourApplication/mount',
                    navIcon: 'InvalidNavIcon',
                    flags: { enabled: true, showInNav: true },
                },
            ],
        };

        const items = getMfeMenuItems(mfeConfig);

        expect(items).toHaveLength(3);

        // Check first item (should use Archive icon)
        expect(items[0].type).toBe(NavBarMenuItemTypes.Item);
        expect(items[0].title).toBe('MFE One');
        expect(items[0].key).toBe('mfe-1');
        expect(items[0].link).toBe('/mfe/mfe-one');
        expect(React.isValidElement(items[0].icon)).toBe(true);
        expect(items[0].icon).not.toBeNull();
        expect((items[0].icon as JSX.Element).type).toBe(Archive);
        expect(typeof items[0].onClick).toBe('function');

        // Check second item (should default to AppWindow icon)
        expect(items[1].type).toBe(NavBarMenuItemTypes.Item);
        expect(items[1].title).toBe('MFE Three');
        expect(items[1].key).toBe('mfe-3');
        expect(items[1].link).toBe('/mfe/mfe-three');
        expect(React.isValidElement(items[1].icon)).toBe(true);
        expect(items[1].icon).not.toBeNull();
        expect((items[1].icon as JSX.Element).type).toBe(AppWindow);
        expect(typeof items[1].onClick).toBe('function');

        // Check third item (should default to AppWindow icon as InvalidNavIcon is not a valid icon)
        expect(items[2].type).toBe(NavBarMenuItemTypes.Item);
        expect(items[2].title).toBe('MFE Four');
        expect(items[2].key).toBe('mfe-4');
        expect(items[2].link).toBe('/mfe/mfe-four');
        expect(React.isValidElement(items[2].icon)).toBe(true);
        expect(items[2].icon).not.toBeNull();
        expect((items[2].icon as JSX.Element).type).toBe(AppWindow);
        expect(typeof items[2].onClick).toBe('function');

        // All should be of type Item
        items.forEach((item) => {
            expect(item.type).toBe(NavBarMenuItemTypes.Item);
        });
    });

    it('returns items of type DropdownElement when subNavigationMode is true', () => {
        const mfeConfig: MFESchema = {
            subNavigationMode: true,
            microFrontends: [
                {
                    id: 'mfe-1',
                    label: 'MFE One',
                    path: '/mfe-one',
                    remoteEntry: 'https://mfe-one.com/remoteEntry.js',
                    module: 'mfeOneApplication/mount',
                    navIcon: 'Archive',
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
        expect(items.length).toBe(2);
        items.forEach((item) => {
            expect(item.type).toBe(NavBarMenuItemTypes.DropdownElement);
        });
    });

    it('returns empty array if no microFrontends (getMfeMenuItems)', () => {
        const mfeConfig: MFESchema = { microFrontends: [], subNavigationMode: false };
        const items = getMfeMenuItems(mfeConfig);
        expect(items).toEqual([]);
    });

    it('returns empty array if no microFrontends (getMfeMenuDropdownItems)', () => {
        const mfeConfig: MFESchema = { microFrontends: [], subNavigationMode: true };
        const items = getMfeMenuDropdownItems(mfeConfig);
        expect(items).toEqual([]);
    });
});
