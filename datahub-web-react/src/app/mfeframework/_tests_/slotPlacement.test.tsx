import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MFEMount } from '@app/mfeframework/MFEConfigurableContainer';
import { MFEConfig, getPlacement, isNavPageMfe, loadMFEConfigFromYAML } from '@app/mfeframework/mfeConfigLoader';
import { EntityDetailTabContext } from '@app/mfeframework/slots/slotTypes';
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

// The visibility registry reaches `logicalModels.utils`, which imports an enum from EntityMenuActions —
// a React component module — for an unrelated menu helper. Stub it to keep this test graph light.
vi.mock('@app/entityV2/shared/EntityDropdown/EntityMenuActions', () => ({ EntityMenuItems: {} }));

/** A pre-slot config entry: exactly what shipped before `placement` existed. */
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
    placement:
      contractVersion: "1.0.0"
      slot: entity.detail.tab
      entityTypes: [dataset, Chart]
      visibleWhen: [physicalDataset]`;

const yaml = (...entries: string[]) => `subNavigationMode: false\nmicroFrontends:${entries.join('')}`;

function tabConfig(overrides: Partial<MFEConfig> = {}): MFEConfig {
    return {
        id: 'access-tab',
        label: 'Access',
        remoteEntry: 'http://localhost:3002/remoteEntry.js',
        module: 'accessMFE/mount',
        flags: { enabled: true },
        placement: { slot: 'entity.detail.tab', contractVersion: '1.0.0', entityTypes: ['dataset'] },
        ...overrides,
    };
}

describe('placement validation', () => {
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    it('accepts an entity.detail.tab entry without path, navIcon or showInNav', () => {
        const schema = loadMFEConfigFromYAML(yaml(TAB_ENTRY));
        expect(schema.microFrontends).toHaveLength(1);
        expect(schema.microFrontends[0].placement).toEqual({
            contractVersion: '1.0.0',
            slot: 'entity.detail.tab',
            entityTypes: ['dataset', 'Chart'],
            visibleWhen: ['physicalDataset'],
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
        const badVisibleWhen = TAB_ENTRY.replace('visibleWhen: [physicalDataset]', 'visibleWhen: [1, 2]').replace(
            'id: access-tab',
            'id: bad-visible',
        );
        const schema = loadMFEConfigFromYAML(yaml(unknownSlot, badTypes, badVisibleWhen, NAV_ENTRY));
        expect(schema.microFrontends.map((c) => c.id)).toEqual(['nav-app']);
    });

    // Gate 1: the host must know which context shape to build before it builds one.
    it('rejects a placement with a missing, blank or non-string contractVersion', () => {
        const missing = TAB_ENTRY.replace('      contractVersion: "1.0.0"\n', '');
        const blank = TAB_ENTRY.replace('contractVersion: "1.0.0"', 'contractVersion: "   "');
        const notAString = TAB_ENTRY.replace('contractVersion: "1.0.0"', 'contractVersion: 1');
        expect(loadMFEConfigFromYAML(yaml(missing)).microFrontends).toHaveLength(0);
        expect(loadMFEConfigFromYAML(yaml(blank)).microFrontends).toHaveLength(0);
        expect(loadMFEConfigFromYAML(yaml(notAString)).microFrontends).toHaveLength(0);
    });

    it('defaults a missing placement to nav.page on the default contract version', () => {
        const [nav] = loadMFEConfigFromYAML(yaml(NAV_ENTRY)).microFrontends;
        expect(getPlacement(nav)).toEqual({ slot: 'nav.page', contractVersion: '1.0.0' });
        expect(isNavPageMfe(nav)).toBe(true);
        expect(isNavPageMfe(tabConfig())).toBe(false);
    });
});

describe('backward compatibility with pre-slot configs', () => {
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    it('parses a legacy nav-only entry unchanged, with no new fields required', () => {
        const schema = loadMFEConfigFromYAML(yaml(NAV_ENTRY));
        expect(schema.microFrontends).toHaveLength(1);
        const [nav] = schema.microFrontends;
        expect(nav).toMatchObject({
            id: 'nav-app',
            label: 'Nav App',
            path: '/nav-app',
            navIcon: 'Globe',
            flags: { enabled: true, showInNav: true },
        });
        expect(nav.placement).toBeUndefined();
        expect(isNavPageMfe(nav)).toBe(true);
        expect(console.error).not.toHaveBeenCalled();
    });

    it('still resolves a legacy entry into the nav.page slot', () => {
        const [nav] = loadMFEConfigFromYAML(yaml(NAV_ENTRY)).microFrontends;
        expect(resolveSlot([nav], 'nav.page')).toEqual([nav]);
    });

    it('accepts an explicit nav.page placement, which still requires the nav fields', () => {
        const explicitNav = `${NAV_ENTRY}
    placement:
      contractVersion: "1.0.0"
      slot: nav.page`;
        const [nav] = loadMFEConfigFromYAML(yaml(explicitNav)).microFrontends;
        expect(isNavPageMfe(nav)).toBe(true);

        const missingNavIcon = explicitNav.replace('    navIcon: Globe\n', '');
        expect(loadMFEConfigFromYAML(yaml(missingNavIcon)).microFrontends).toHaveLength(0);
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

    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    it('returns only enabled entries placed in the requested slot', () => {
        const disabled = tabConfig({ id: 'off', flags: { enabled: false } });
        expect(resolveSlot([nav, tabConfig(), disabled], 'entity.detail.tab', { entityType: 'DATASET' })).toEqual([
            tabConfig(),
        ]);
        expect(resolveSlot([nav, tabConfig()], 'nav.page')).toEqual([nav]);
    });

    it('matches entityTypes case-insensitively', () => {
        expect(matchesEntityType(tabConfig(), 'DATASET')).toBe(true);
        const upper = tabConfig({
            placement: { slot: 'entity.detail.tab', contractVersion: '1.0.0', entityTypes: ['DATASET'] },
        });
        expect(matchesEntityType(upper, 'dataset')).toBe(true);
        expect(matchesEntityType(tabConfig(), 'CHART')).toBe(false);
        expect(resolveSlot([tabConfig()], 'entity.detail.tab', { entityType: 'chart' })).toEqual([]);
    });

    it('matches a multi-word entity type by its enum value', () => {
        const dataFlow = tabConfig({
            placement: { slot: 'entity.detail.tab', contractVersion: '1.0.0', entityTypes: ['data_flow'] },
        });
        expect(matchesEntityType(dataFlow, 'DATA_FLOW')).toBe(true);
        expect(matchesEntityType(dataFlow, 'DATAFLOW')).toBe(false);
    });

    // Layer 1 fails closed: appearing on a page is an explicit opt-in.
    it('shows nowhere when an entity-scoped placement omits entityTypes', () => {
        const noFilter = tabConfig({ placement: { slot: 'entity.detail.tab', contractVersion: '1.0.0' } });
        expect(matchesEntityType(noFilter, 'DATASET')).toBe(false);
        expect(resolveSlot([noFilter], 'entity.detail.tab', { entityType: 'DATASET' })).toEqual([]);
        expect(resolveSlot([noFilter], 'entity.detail.tab')).toEqual([]);
    });

    // Gate 2: a version the host has no builder for never produces a tab.
    it('drops an entry whose contractVersion the host cannot build', () => {
        const future = tabConfig({
            placement: { slot: 'entity.detail.tab', contractVersion: '9.9.9', entityTypes: ['dataset'] },
        });
        expect(resolveSlot([future], 'entity.detail.tab', { entityType: 'DATASET' })).toEqual([]);
        expect(console.error).toHaveBeenCalledWith(expect.stringContaining('9.9.9'));
    });
});

describe('mfeConfigToEntityTab', () => {
    it('names the tab from the generic label and addresses it by a stable route key', () => {
        const tab = mfeConfigToEntityTab(tabConfig(), 'DATASET');
        expect(tab.name).toBe('Access');
        expect(tab.id).toBe('mfe-access-tab');
        // Addressing is independent of the caption, so renaming the label keeps deep links working.
        expect(tab.routeKey).toBe('mfe-access-tab');
        const renamed = mfeConfigToEntityTab(tabConfig({ label: 'Renamed' }), 'DATASET');
        expect(renamed.name).toBe('Renamed');
        expect(renamed.routeKey).toBe('mfe-access-tab');
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
            contractVersion: '1.0.0',
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
