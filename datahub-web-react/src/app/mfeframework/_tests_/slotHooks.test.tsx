import { act, render, screen, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { UserContext } from '@app/context/userContext';
import { EntityContext } from '@app/entityV2/shared/EntityContext';
import { EntitySidebarSection, EntityTab } from '@app/entityV2/shared/types';
import { MFEConfigContext } from '@app/mfeframework/MFEConfigContext';
import MFEConfigProvider from '@app/mfeframework/MFEConfigProvider';
import { MFEBaseConfigurablePage, MFEMount } from '@app/mfeframework/MFEConfigurableContainer';
import {
    MFEConfig,
    MFESchema,
    loadMFEConfigFromYAML,
    useMFEConfig,
    useMFEConfigFetch,
} from '@app/mfeframework/mfeConfigLoader';
import { getMfeMenuDropdownItems, getMfeMenuItems } from '@app/mfeframework/mfeNavBarMenuUtils';
import MFEEntityRegion from '@app/mfeframework/slots/MFEEntityRegion';
import MFEEntityTab from '@app/mfeframework/slots/MFEEntityTab';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';
import { DEFAULT_SLOT_CONTRACT_VERSION, isMFESlotId } from '@app/mfeframework/slots/slotTypes';
import { insertSlotTabs, useMFEEntityTabs } from '@app/mfeframework/slots/useMFEEntityTabs';
import { useMFESidebarSections } from '@app/mfeframework/slots/useMFESidebarSections';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';
import * as navBarHooks from '@app/useShowNavBarRedesign';

import { EntityType } from '@types';

const { getRemoteMock, setRemoteMock, unwrapModuleMock } = vi.hoisted(() => ({
    getRemoteMock: vi.fn(),
    setRemoteMock: vi.fn(),
    unwrapModuleMock: vi.fn(),
}));

vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(true);

vi.mock('virtual:__federation__', () => ({
    __federation_method_getRemote: getRemoteMock,
    __federation_method_setRemote: setRemoteMock,
    __federation_method_unwrapDefault: unwrapModuleMock,
}));

// The visibility registry reaches `logicalModels.utils`, which imports an enum from EntityMenuActions —
// a React component module — for an unrelated menu helper. Stub it to keep this test graph light.
vi.mock('@app/entityV2/shared/EntityDropdown/EntityMenuActions', () => ({ EntityMenuItems: {} }));

const THEME = { styles: {}, colors: { bg: 'none' }, assets: {}, content: {} };
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)';

const TAB: MFEConfig = {
    id: 'access-tab',
    label: 'Access',
    remoteEntry: 'http://localhost:3002/remoteEntry.js',
    module: 'accessMFE/mount',
    flags: { enabled: true },
    placement: { slot: 'entity.detail.tab', contractVersion: '1.0.0', entityTypes: ['dataset'] },
};
const NAV: MFEConfig = {
    id: 'nav-app',
    label: 'Nav App',
    path: '/nav-app',
    remoteEntry: 'http://localhost:3002/remoteEntry.js',
    module: 'navApp/mount',
    flags: { enabled: true, showInNav: true },
    navIcon: 'Globe',
};
const SCHEMA: MFESchema = { topLevelMenuTitle: 'Apps', subNavigationMode: false, microFrontends: [NAV, TAB] };

/** The context the host builds for a legacy nav.page entry. */
const NAV_CTX = {
    slot: 'nav.page',
    contractVersion: DEFAULT_SLOT_CONTRACT_VERSION,
} as const;

const YAML = `subNavigationMode: false
microFrontends:
  - id: nav-app
    label: Nav App
    path: /nav-app
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: navApp/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: Globe`;

function withProviders(ui: React.ReactElement, { user = 'urn:li:corpuser:jdoe' }: { user?: string | null } = {}) {
    return (
        <MemoryRouter>
            <ThemeProvider theme={THEME as any}>
                <UserContext.Provider value={{ urn: user } as any}>{ui}</UserContext.Provider>
            </ThemeProvider>
        </MemoryRouter>
    );
}

function mockFetch(body: string, ok = true) {
    global.fetch = vi.fn().mockResolvedValue({ ok, statusText: ok ? 'OK' : 'Boom', text: () => Promise.resolve(body) });
}

describe('MFEConfigProvider / useMFEConfig', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });
    afterEach(() => vi.restoreAllMocks());

    it('fetches once and shares the parsed config with consumers', async () => {
        mockFetch(YAML);
        const Consumer = () => {
            const { config, loading } = useMFEConfig();
            return <div data-testid="out">{loading ? 'loading' : (config?.microFrontends.length ?? 'none')}</div>;
        };
        render(
            <MFEConfigProvider>
                <Consumer />
                <Consumer />
            </MFEConfigProvider>,
        );
        await waitFor(() => expect(screen.getAllByTestId('out')[0]).toHaveTextContent('1'));
        expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    it('falls back to fetching itself when no provider is mounted', async () => {
        mockFetch(YAML);
        const { result, waitForNextUpdate } = renderHook(() => useMFEConfig());
        expect(result.current.loading).toBe(true);
        await waitForNextUpdate();
        expect(result.current.config?.microFrontends).toHaveLength(1);
    });

    it('reports loading=false and config=null when the fetch fails', async () => {
        mockFetch('nope', false);
        const { result, waitForNextUpdate } = renderHook(() => useMFEConfigFetch());
        await waitForNextUpdate();
        expect(result.current).toEqual({ config: null, loading: false });
    });

    it('does nothing when asked to skip', () => {
        global.fetch = vi.fn();
        const { result } = renderHook(() => useMFEConfigFetch(true));
        expect(result.current).toEqual({ config: null, loading: false });
        expect(global.fetch).not.toHaveBeenCalled();
    });
});

describe('placement validation edge cases', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });
    afterEach(() => vi.restoreAllMocks());

    it('rejects a non-object placement and a placement missing its contract version', () => {
        const base = `subNavigationMode: false\nmicroFrontends:\n  - id: x\n    label: X\n    remoteEntry: http://r/remoteEntry.js\n    module: m/mount\n    flags:\n      enabled: true\n      showInNav: false\n`;
        expect(loadMFEConfigFromYAML(`${base}    placement: entity.detail.tab\n`).microFrontends).toHaveLength(0);
        expect(
            loadMFEConfigFromYAML(`${base}    placement:\n      slot: entity.detail.tab\n`).microFrontends,
        ).toHaveLength(0);
        expect(
            loadMFEConfigFromYAML(
                `${base}    placement:\n      slot: entity.detail.tab\n      contractVersion: "1.0.0"\n      visibleWhen: notAnArray\n`,
            ).microFrontends,
        ).toHaveLength(0);
    });

    it('isMFESlotId only accepts known slot names', () => {
        expect(isMFESlotId('nav.page')).toBe(true);
        expect(isMFESlotId('entity.detail.tab')).toBe(true);
        expect(isMFESlotId('sidebar.widget')).toBe(false);
        expect(isMFESlotId(42)).toBe(false);
    });
});

describe('slot hooks', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <MFEConfigContext.Provider value={{ provided: true, config: SCHEMA, loading: false }}>
            {children}
        </MFEConfigContext.Provider>
    );

    it('useResolveSlot reads the shared config', () => {
        const { result } = renderHook(() => useResolveSlot('entity.detail.tab', { entityType: 'DATASET' }), {
            wrapper,
        });
        expect(result.current).toEqual([TAB]);
    });

    it('useMFEEntityTabs maps placed entries to EntityTab definitions for the entity type', () => {
        const { result } = renderHook(() => useMFEEntityTabs('DATASET'), { wrapper });
        expect(result.current.map((t) => t.name)).toEqual(['Access']);
        expect(result.current[0].display?.visible(null as any, null)).toBe(true);
        expect(result.current[0].display?.enabled(null as any, null)).toBe(true);
        const { result: none } = renderHook(() => useMFEEntityTabs('CHART'), { wrapper });
        expect(none.current).toEqual([]);
    });

    it('hides a tab whose visibleWhen predicate rejects the entity on screen', () => {
        const gated: MFESchema = {
            ...SCHEMA,
            microFrontends: [{ ...TAB, placement: { ...TAB.placement!, visibleWhen: ['physicalDataset'] } }],
        };
        const gatedWrapper = ({ children }: { children: React.ReactNode }) => (
            <MFEConfigContext.Provider value={{ provided: true, config: gated, loading: false }}>
                {children}
            </MFEConfigContext.Provider>
        );
        const { result } = renderHook(() => useMFEEntityTabs('DATASET'), { wrapper: gatedWrapper });
        expect(result.current).toHaveLength(1);
        const logical = { platform: { properties: { logical: true } } } as any;
        expect(result.current[0].display?.visible(logical, null)).toBe(false);
        expect(result.current[0].display?.visible({ platform: {} } as any, null)).toBe(true);
    });

    it('useMFEEntityTabs renders a lazy icon when navIcon is set', () => {
        const withIcon: MFESchema = { ...SCHEMA, microFrontends: [{ ...TAB, navIcon: 'Lock' }] };
        const iconWrapper = ({ children }: { children: React.ReactNode }) => (
            <MFEConfigContext.Provider value={{ provided: true, config: withIcon, loading: false }}>
                {children}
            </MFEConfigContext.Provider>
        );
        const { result } = renderHook(() => useMFEEntityTabs('DATASET'), { wrapper: iconWrapper });
        const Icon = result.current[0].icon as React.FunctionComponent;
        expect(Icon).toBeDefined();
        expect(React.isValidElement(Icon({}))).toBe(true);
    });
});

describe('MFEEntityTab and MFEBaseConfigurablePage contexts', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(true);
    });
    afterEach(() => vi.restoreAllMocks());

    function mountResolvesTo(mod: unknown) {
        getRemoteMock.mockResolvedValue(mod);
        unwrapModuleMock.mockResolvedValue(mod);
    }

    it('MFEEntityTab builds the entity.detail.tab context from the entity page', async () => {
        const mountFn = vi.fn(() => vi.fn());
        mountResolvesTo({ mount: mountFn });
        await act(async () => {
            render(
                withProviders(
                    <EntityContext.Provider
                        value={
                            {
                                urn: DATASET_URN,
                                entityType: EntityType.Dataset,
                                entityData: null,
                                loading: false,
                            } as any
                        }
                    >
                        <MFEEntityTab config={TAB} />
                    </EntityContext.Provider>,
                ),
            );
        });
        expect(mountFn).toHaveBeenCalledWith(screen.getByTestId('mfe-slot-container'), {
            slot: 'entity.detail.tab',
            contractVersion: DEFAULT_SLOT_CONTRACT_VERSION,
            entity: { urn: DATASET_URN, type: 'DATASET' },
            principal: { user: 'urn:li:corpuser:jdoe' },
        });
    });

    it('omits principal when the user urn is unknown', async () => {
        const mountFn = vi.fn(() => vi.fn());
        mountResolvesTo({ mount: mountFn });
        await act(async () => {
            render(withProviders(<MFEBaseConfigurablePage config={NAV} />, { user: null }));
        });
        expect(mountFn).toHaveBeenCalledWith(expect.anything(), {
            slot: 'nav.page',
            contractVersion: DEFAULT_SLOT_CONTRACT_VERSION,
        });
    });

    it('accepts a module whose default export is the mount function and tolerates a non-function return', async () => {
        const mountFn = vi.fn(() => 'not-a-cleanup');
        mountResolvesTo({ default: mountFn });
        const ctx = NAV_CTX;
        let view: ReturnType<typeof render> | undefined;
        await act(async () => {
            view = render(withProviders(<MFEMount config={NAV} ctx={ctx} />));
        });
        expect(mountFn).toHaveBeenCalledTimes(1);
        await act(async () => view?.unmount());
    });

    it('accepts a module whose default export exposes mount', async () => {
        const mountFn = vi.fn(() => vi.fn());
        mountResolvesTo({ default: { mount: mountFn } });
        const ctx = NAV_CTX;
        await act(async () => {
            render(withProviders(<MFEMount config={NAV} ctx={ctx} />));
        });
        expect(mountFn).toHaveBeenCalledTimes(1);
    });

    it('does not call a module that exposes no mount function', async () => {
        mountResolvesTo({ somethingElse: true });
        const ctx = NAV_CTX;
        await act(async () => {
            render(withProviders(<MFEMount config={NAV} ctx={ctx} />));
        });
        expect(console.warn).toHaveBeenCalledWith('[HOST] mount is not a function; got: ', undefined);
    });

    it('skips mounting when the component unmounts before the remote resolves', async () => {
        const mountFn = vi.fn(() => vi.fn());
        let release: (v: unknown) => void = () => {};
        getRemoteMock.mockReturnValue(
            new Promise((resolve) => {
                release = resolve;
            }),
        );
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        const ctx = NAV_CTX;
        const view = render(withProviders(<MFEMount config={NAV} ctx={ctx} />));
        view.unmount();
        await act(async () => {
            release({ mount: mountFn });
        });
        expect(mountFn).not.toHaveBeenCalled();
    });

    it('does not mount a disabled entry even if the remote resolves', async () => {
        const mountFn = vi.fn(() => vi.fn());
        mountResolvesTo({ mount: mountFn });
        const disabled = { ...NAV, flags: { enabled: false, showInNav: true } };
        const ctx = NAV_CTX;
        await act(async () => {
            render(withProviders(<MFEMount config={disabled} ctx={ctx} />));
        });
        expect(mountFn).not.toHaveBeenCalled();
        expect(screen.queryByTestId('mfe-slot-container')).toBeNull();
    });
});

describe('navigation menu items', () => {
    it('only lists nav.page entries and logs on click', () => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
        const items = getMfeMenuItems(SCHEMA);
        expect(items.map((i) => i.key)).toEqual(['nav-app']);
        items[0].onClick?.();
        const dropdown = getMfeMenuDropdownItems(SCHEMA);
        expect(dropdown.map((i) => i.key)).toEqual(['nav-app']);
        dropdown[0].onClick?.();
        expect(console.log).toHaveBeenCalledTimes(2);
        vi.restoreAllMocks();
    });
});

describe('remaining validation and fallback branches', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(false);
    });
    afterEach(() => vi.restoreAllMocks());

    it('rejects entries whose core fields have the wrong types', () => {
        const bad = `subNavigationMode: false
microFrontends:
  - id: 1
    label: 2
    path: /x
    remoteEntry: 3
    module: m/mount
    flags: nope
    navIcon: Globe
  - id: y
    label: Y
    path: /y
    remoteEntry: http://r/remoteEntry.js
    module: m/mount
    flags:
      enabled: yes please
      showInNav: 1
    navIcon: Globe`;
        expect(loadMFEConfigFromYAML(bad).microFrontends).toHaveLength(0);
    });

    it('menu utils tolerate a missing list and a missing navIcon', () => {
        expect(getMfeMenuItems(undefined as any)).toEqual([]);
        expect(getMfeMenuDropdownItems({} as any)).toEqual([]);
        const noIcon: MFESchema = { ...SCHEMA, microFrontends: [{ ...NAV, navIcon: undefined }] };
        expect(getMfeMenuItems(noIcon)).toHaveLength(1);
        expect(getMfeMenuDropdownItems(noIcon)).toHaveLength(1);
    });

    it('a filtered slot never matches when the page has no entity type, and a null config resolves to nothing', () => {
        const { result: filtered } = renderHook(() => useResolveSlot('entity.detail.tab'), {
            wrapper: ({ children }: { children: React.ReactNode }) => (
                <MFEConfigContext.Provider value={{ provided: true, config: SCHEMA, loading: false }}>
                    {children}
                </MFEConfigContext.Provider>
            ),
        });
        expect(filtered.current).toEqual([]);
        const { result: empty } = renderHook(() => useResolveSlot('nav.page'), {
            wrapper: ({ children }: { children: React.ReactNode }) => (
                <MFEConfigContext.Provider value={{ provided: true, config: null, loading: false }}>
                    {children}
                </MFEConfigContext.Provider>
            ),
        });
        expect(empty.current).toEqual([]);
    });

    it('MFEEntityTab omits principal for an anonymous viewer and accepts a bare function module', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue(mountFn);
        unwrapModuleMock.mockResolvedValue(mountFn);
        await act(async () => {
            render(
                withProviders(
                    <EntityContext.Provider
                        value={
                            { urn: DATASET_URN, entityType: EntityType.Chart, entityData: null, loading: false } as any
                        }
                    >
                        <MFEEntityTab config={TAB} />
                    </EntityContext.Provider>,
                    { user: null },
                ),
            );
        });
        expect(mountFn).toHaveBeenCalledWith(expect.anything(), {
            slot: 'entity.detail.tab',
            contractVersion: DEFAULT_SLOT_CONTRACT_VERSION,
            entity: { urn: DATASET_URN, type: 'CHART' },
        });
    });

    it('renders nothing when the entry declares a contract version the host cannot build', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        const future: MFEConfig = { ...TAB, placement: { ...TAB.placement!, contractVersion: '4.0.0' } };
        await act(async () => {
            render(
                withProviders(
                    <EntityContext.Provider
                        value={
                            {
                                urn: DATASET_URN,
                                entityType: EntityType.Dataset,
                                entityData: null,
                                loading: false,
                            } as any
                        }
                    >
                        <MFEEntityTab config={future} />
                    </EntityContext.Provider>,
                ),
            );
        });
        expect(mountFn).not.toHaveBeenCalled();
        expect(screen.queryByTestId('mfe-slot-container')).toBeNull();
    });
});

describe('slot edge cases', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(true);
    });
    afterEach(() => vi.restoreAllMocks());

    it('rejects a non-boolean showInNav on a slot entry, where the flag is otherwise optional', () => {
        const yaml = `subNavigationMode: false
microFrontends:
  - id: tab
    label: Tab
    remoteEntry: http://r/remoteEntry.js
    module: m/mount
    flags:
      enabled: true
      showInNav: sometimes
    placement:
      slot: entity.detail.tab
      contractVersion: "1.0.0"
      entityTypes: [dataset]`;
        expect(loadMFEConfigFromYAML(yaml).microFrontends).toHaveLength(0);
        expect(loadMFEConfigFromYAML(yaml.replace('      showInNav: sometimes\n', '')).microFrontends).toHaveLength(1);
    });

    it('builds no entity tab context until the page has resolved the entity', () => {
        expect(buildSlotContext(TAB, {})).toBeNull();
        expect(buildSlotContext(TAB, { urn: 'urn:li:dataset:x' })).toBeNull();
    });

    it('does not mount a navigation page whose contract version the host cannot build', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        const unsupported: MFEConfig = { ...NAV, placement: { slot: 'nav.page', contractVersion: '9.9.9' } };
        await act(async () => {
            render(withProviders(<MFEBaseConfigurablePage config={unsupported} />));
        });
        expect(mountFn).not.toHaveBeenCalled();
        expect(screen.queryByTestId('mfe-configurable-container')).toBeNull();
    });
});

describe('slot tab position (placement.after)', () => {
    const tab = (path: string) => ({ path, name: path, component: () => null }) as EntityTab;
    const host = [tab('Summary'), tab('Columns'), tab('Lineage')];
    const paths = (tabs: EntityTab[]) => tabs.map((t) => t.path);

    it('places a tab right after the named host tab, matching its path case-insensitively', () => {
        expect(paths(insertSlotTabs(host, [{ tab: tab('mfe-a'), after: 'summary' }]))).toEqual([
            'Summary',
            'mfe-a',
            'Columns',
            'Lineage',
        ]);
    });

    it('keeps config order for several tabs after the same target', () => {
        const placed = insertSlotTabs(host, [
            { tab: tab('mfe-a'), after: 'Summary' },
            { tab: tab('mfe-b'), after: 'Summary' },
        ]);
        expect(paths(placed)).toEqual(['Summary', 'mfe-a', 'mfe-b', 'Columns', 'Lineage']);
    });

    it('appends a tab with no target, or with a target the page does not have', () => {
        const placed = insertSlotTabs(host, [{ tab: tab('mfe-a'), after: 'NoSuchTab' }, { tab: tab('mfe-b') }]);
        expect(paths(placed)).toEqual(['Summary', 'Columns', 'Lineage', 'mfe-a', 'mfe-b']);
    });

    it('useMFEEntityTabs merges slot tabs into the host tabs at their configured position', () => {
        const positioned: MFESchema = {
            ...SCHEMA,
            microFrontends: [{ ...TAB, placement: { ...TAB.placement!, after: 'Columns' } }],
        };
        const positionedWrapper = ({ children }: { children: React.ReactNode }) => (
            <MFEConfigContext.Provider value={{ provided: true, config: positioned, loading: false }}>
                {children}
            </MFEConfigContext.Provider>
        );
        const { result } = renderHook(() => useMFEEntityTabs('DATASET', host), { wrapper: positionedWrapper });
        expect(paths(result.current)).toEqual(['Summary', 'Columns', 'mfe-access-tab', 'Lineage']);
    });

    it('rejects an entry whose placement.after is not a non-empty string', () => {
        const entry = (after: string) => `subNavigationMode: false
microFrontends:
  - id: tab
    label: Tab
    remoteEntry: http://r/remoteEntry.js
    module: m/mount
    flags:
      enabled: true
    placement:
      slot: entity.detail.tab
      contractVersion: "1.0.0"
      entityTypes: [dataset]
      after: ${after}`;
        expect(loadMFEConfigFromYAML(entry('Summary')).microFrontends).toHaveLength(1);
        expect(loadMFEConfigFromYAML(entry('42')).microFrontends).toHaveLength(0);
        expect(loadMFEConfigFromYAML(entry('"  "')).microFrontends).toHaveLength(0);
    });
});

describe('Summary tab regions (entity.summary.top / entity.summary.bottom)', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(navBarHooks, 'useShowNavBarRedesign').mockReturnValue(true);
    });
    afterEach(() => vi.restoreAllMocks());

    const region = (id: string, slot: 'entity.summary.top' | 'entity.summary.bottom', extra = {}): MFEConfig => ({
        ...TAB,
        id,
        placement: { slot, contractVersion: '1.0.0', entityTypes: ['dataset'], ...extra },
    });

    function renderRegion(
        slot: 'entity.summary.top' | 'entity.summary.bottom',
        microFrontends: MFEConfig[],
        entityData: unknown = { platform: {} },
    ) {
        const schema: MFESchema = { ...SCHEMA, microFrontends };
        return render(
            withProviders(
                <MFEConfigContext.Provider value={{ provided: true, config: schema, loading: false }}>
                    <EntityContext.Provider
                        value={{ urn: DATASET_URN, entityType: EntityType.Dataset, entityData, loading: false } as any}
                    >
                        <MFEEntityRegion slot={slot} />
                    </EntityContext.Provider>
                </MFEConfigContext.Provider>,
            ),
        );
    }

    it('mounts each entry placed in the region with the region slot in its context', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        await act(async () => {
            renderRegion('entity.summary.top', [
                region('top-a', 'entity.summary.top'),
                region('bottom-a', 'entity.summary.bottom'),
                TAB,
            ]);
        });
        expect(mountFn).toHaveBeenCalledTimes(1);
        expect(mountFn).toHaveBeenCalledWith(expect.anything(), {
            slot: 'entity.summary.top',
            contractVersion: '1.0.0',
            entity: { urn: DATASET_URN, type: 'DATASET' },
            principal: { user: 'urn:li:corpuser:jdoe' },
        });
    });

    it('renders nothing when no entry applies, including one hidden by visibleWhen', async () => {
        await act(async () => {
            renderRegion('entity.summary.bottom', [
                region('bottom-logical', 'entity.summary.bottom', { visibleWhen: ['logicalDataset'] }),
            ]);
        });
        expect(screen.queryByTestId('mfe-region-entity.summary.bottom')).toBeNull();
        expect(getRemoteMock).not.toHaveBeenCalled();
    });

    it('requires an entity type opt-in like any other entity-scoped slot', () => {
        expect(isMFESlotId('entity.summary.top')).toBe(true);
        const noTypes = region('top', 'entity.summary.top', { entityTypes: undefined });
        const { result } = renderHook(() => useResolveSlot('entity.summary.top', { entityType: 'DATASET' }), {
            wrapper: ({ children }: { children: React.ReactNode }) => (
                <MFEConfigContext.Provider
                    value={{ provided: true, config: { ...SCHEMA, microFrontends: [noTypes] }, loading: false }}
                >
                    {children}
                </MFEConfigContext.Provider>
            ),
        });
        expect(result.current).toEqual([]);
    });
});

describe('sidebar sections (entity.sidebar.section)', () => {
    const section = (key: string) => ({ key, component: () => null }) as EntitySidebarSection;
    const host = [section('about'), section('notes'), section('logical'), section('owners')];
    const sidebarEntry = (id: string, after?: string, visibleWhen?: string[]): MFEConfig => ({
        ...TAB,
        id,
        placement: {
            slot: 'entity.sidebar.section',
            contractVersion: '1.0.0',
            entityTypes: ['dataset'],
            after,
            visibleWhen,
        },
    });

    function renderSections(microFrontends: MFEConfig[]) {
        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <MFEConfigContext.Provider
                value={{ provided: true, config: { ...SCHEMA, microFrontends }, loading: false }}
            >
                <EntityContext.Provider
                    value={
                        { urn: DATASET_URN, entityType: EntityType.Dataset, entityData: null, loading: false } as any
                    }
                >
                    {children}
                </EntityContext.Provider>
            </MFEConfigContext.Provider>
        );
        return renderHook(() => useMFESidebarSections(host), { wrapper }).result.current;
    }

    it('places a section right after the host section its placement names, else at the end', () => {
        const sections = renderSections([sidebarEntry('custom', 'notes'), sidebarEntry('tail'), TAB]);
        expect(sections.map((s) => s.key)).toEqual(['about', 'notes', 'mfe-custom', 'logical', 'owners', 'mfe-tail']);
    });

    it('gates each section on visibleWhen against the entity on screen', () => {
        const [, , gated] = renderSections([sidebarEntry('custom', 'notes', ['logicalDataset'])]);
        expect(gated.display?.visible({ platform: { properties: { logical: true } } } as any, null)).toBe(true);
        expect(gated.display?.visible({ platform: {} } as any, null)).toBe(false);
    });
});
