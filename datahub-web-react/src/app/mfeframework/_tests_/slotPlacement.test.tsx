import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MFEMount } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig, getPlacement, isNavPageMfe, loadMFEConfigFromYAML } from '@app/mfeframework/mfeConfigLoader';
import { EntityDetailTabContext, SLOT_CONTRACT_VERSION } from '@app/mfeframework/slots/slotTypes';
import { mfeConfigToEntityTab } from '@app/mfeframework/slots/useMFEEntityTabs';
import { matchesEntityType, resolveSlot } from '@app/mfeframework/slots/useResolveSlot';

const { getRemoteMock, setRemoteMock, unwrapModuleMock } = vi.hoisted(() => ({
    getRemoteMock: vi.fn(),
    setRemoteMock: vi.fn(),
    unwrapModuleMock: vi.fn(),
}));

vi.mock('virtual:__federation__', () => ({
    __federation_method_getRemote: getRemoteMock,
    __federation_method_setRemote: setRemoteMock,
    __federation_method_unwrapDefault: unwrapModuleMock,
}));

const NAV_ENTRY = `
  - id: nav-app
    label: Nav App
    path: /nav-app
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: navApp/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: Globe`;

const TAB_ENTRY = `
  - id: access-tab
    label: Access
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: accessMFE/mount
    flags:
      enabled: true
      showInNav: false
    placement:
      slot: entity.detail.tab
      entityTypes: [dataset, Chart]
      tabName: "Access Requests"`;

const yaml = (...entries: string[]) => `subNavigationMode: false\nmicroFrontends:${entries.join('')}`;

function tabConfig(overrides: Partial<MFEConfig> = {}): MFEConfig {
    return {
        id: 'access-tab',
        label: 'Access',
        remoteEntry: 'http://localhost:3002/remoteEntry.js',
        module: 'accessMFE/mount',
        flags: { enabled: true, showInNav: false },
        placement: { slot: 'entity.detail.tab', entityTypes: ['dataset'], tabName: 'Access Requests' },
        ...overrides,
    };
}

describe('placement validation', () => {
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    it('accepts an entity.detail.tab entry without path or navIcon', () => {
        const schema = loadMFEConfigFromYAML(yaml(TAB_ENTRY));
        expect(schema.microFrontends).toHaveLength(1);
        expect(schema.microFrontends[0].placement).toEqual({
            slot: 'entity.detail.tab',
            entityTypes: ['dataset', 'Chart'],
            tabName: 'Access Requests',
        });
    });

    it('still requires path and navIcon for nav.page entries', () => {
        const missingPath = NAV_ENTRY.replace('    path: /nav-app\n', '');
        expect(loadMFEConfigFromYAML(yaml(missingPath)).microFrontends).toHaveLength(0);
        expect(loadMFEConfigFromYAML(yaml(NAV_ENTRY)).microFrontends).toHaveLength(1);
    });

    it('drops entries with an unknown slot or malformed placement fields, keeping the rest', () => {
        const unknownSlot = TAB_ENTRY.replace('slot: entity.detail.tab', 'slot: sidebar.widget');
        const badTypes = TAB_ENTRY.replace('entityTypes: [dataset, Chart]', 'entityTypes: dataset').replace(
            'id: access-tab',
            'id: bad-types',
        );
        const schema = loadMFEConfigFromYAML(yaml(unknownSlot, badTypes, NAV_ENTRY));
        expect(schema.microFrontends.map((c) => c.id)).toEqual(['nav-app']);
    });

    it('defaults a missing placement to nav.page', () => {
        const [nav] = loadMFEConfigFromYAML(yaml(NAV_ENTRY)).microFrontends;
        expect(getPlacement(nav)).toEqual({ slot: 'nav.page' });
        expect(isNavPageMfe(nav)).toBe(true);
        expect(isNavPageMfe(tabConfig())).toBe(false);
    });
});

describe('resolveSlot', () => {
    const nav: MFEConfig = {
        id: 'nav-app',
        label: 'Nav App',
        path: '/nav-app',
        remoteEntry: 'http://localhost:3002/remoteEntry.js',
        module: 'navApp/mount',
        flags: { enabled: true, showInNav: true },
        navIcon: 'Globe',
    };

    it('returns only enabled entries placed in the requested slot', () => {
        const disabled = tabConfig({ id: 'off', flags: { enabled: false, showInNav: false } });
        expect(resolveSlot([nav, tabConfig(), disabled], 'entity.detail.tab', { entityType: 'DATASET' })).toEqual([
            tabConfig(),
        ]);
        expect(resolveSlot([nav, tabConfig()], 'nav.page')).toEqual([nav]);
    });

    it('matches entityTypes case-insensitively and treats a missing filter as match-all', () => {
        expect(matchesEntityType(tabConfig(), 'DATASET')).toBe(true);
        expect(matchesEntityType(tabConfig(), 'CHART')).toBe(false);
        expect(matchesEntityType(tabConfig({ placement: { slot: 'entity.detail.tab' } }), 'CHART')).toBe(true);
        expect(resolveSlot([tabConfig()], 'entity.detail.tab', { entityType: 'chart' })).toEqual([]);
    });
});

describe('mfeConfigToEntityTab', () => {
    it('names the tab from placement.tabName, falling back to label', () => {
        expect(mfeConfigToEntityTab(tabConfig()).name).toBe('Access Requests');
        expect(mfeConfigToEntityTab(tabConfig({ placement: { slot: 'entity.detail.tab' } })).name).toBe('Access');
        expect(mfeConfigToEntityTab(tabConfig()).id).toBe('mfe-access-tab');
    });
});

describe('MFEMount', () => {
    const theme = { styles: {}, colors: { bg: 'none' }, assets: {}, content: {} };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    it('hands the typed slot context to the remote mount function', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        const ctx: EntityDetailTabContext = {
            slot: 'entity.detail.tab',
            version: SLOT_CONTRACT_VERSION,
            entity: { urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)', type: 'DATASET' },
            principal: { user: 'urn:li:corpuser:jdoe' },
        };

        await act(async () => {
            render(
                <MemoryRouter>
                    <ThemeProvider theme={theme as any}>
                        <MFEMount config={tabConfig()} ctx={ctx} />
                    </ThemeProvider>
                </MemoryRouter>,
            );
        });

        const container = screen.getByTestId('mfe-slot-container');
        expect(setRemoteMock).toHaveBeenCalledWith('accessMFE', expect.objectContaining({ format: 'var' }));
        expect(mountFn).toHaveBeenCalledWith(container, ctx);
    });
});
