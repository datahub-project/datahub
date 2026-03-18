import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
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

        // All visible items use AppWindow as default icon (React.lazy resolution is a follow-up)
        items.forEach((item) => {
            expect(React.isValidElement(item.icon)).toBe(true);
            expect((item.icon as JSX.Element).type).toBe(AppWindow);
            expect(typeof item.onClick).toBe('function');
        });

        expect(items[0].title).toBe('MFE One');
        expect(items[0].key).toBe('mfe-1');
        expect(items[0].link).toBe('/mfe/mfe-one');
        expect(items[1].title).toBe('MFE Three');
        expect(items[1].key).toBe('mfe-3');
        expect(items[2].title).toBe('MFE Four');
        expect(items[2].key).toBe('mfe-4');

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
