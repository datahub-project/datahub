import { act, render, screen, waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { UserContext } from '@app/context/userContext';
import { EntityContext } from '@app/entity/shared/EntityContext';
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
import MFEEntityTab from '@app/mfeframework/slots/MFEEntityTab';
import { SLOT_CONTRACT_VERSION, isMFESlotId } from '@app/mfeframework/slots/slotTypes';
import { useMFEEntityTabs } from '@app/mfeframework/slots/useMFEEntityTabs';
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

const THEME = { styles: {}, colors: { bg: 'none' }, assets: {}, content: {} };
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)';

const TAB: MFEConfig = {
    id: 'access-tab',
    label: 'Access',
    remoteEntry: 'http://localhost:3002/remoteEntry.js',
    module: 'accessMFE/mount',
    flags: { enabled: true, showInNav: false },
    placement: { slot: 'entity.detail.tab', entityTypes: ['dataset'], tabName: 'Access Requests' },
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

    it('rejects a non-object placement and a non-string tabName', () => {
        const base = `subNavigationMode: false\nmicroFrontends:\n  - id: x\n    label: X\n    remoteEntry: http://r/remoteEntry.js\n    module: m/mount\n    flags:\n      enabled: true\n      showInNav: false\n`;
        expect(loadMFEConfigFromYAML(`${base}    placement: entity.detail.tab\n`).microFrontends).toHaveLength(0);
        expect(
            loadMFEConfigFromYAML(`${base}    placement:\n      slot: entity.detail.tab\n      tabName: 42\n`)
                .microFrontends,
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
        expect(result.current.map((t) => t.name)).toEqual(['Access Requests']);
        expect(result.current[0].display?.visible(null as any, null)).toBe(true);
        expect(result.current[0].display?.enabled(null as any, null)).toBe(true);
        const { result: none } = renderHook(() => useMFEEntityTabs('CHART'), { wrapper });
        expect(none.current).toEqual([]);
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
            version: SLOT_CONTRACT_VERSION,
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
        expect(mountFn).toHaveBeenCalledWith(expect.anything(), { slot: 'nav.page', version: SLOT_CONTRACT_VERSION });
    });

    it('accepts a module whose default export is the mount function and tolerates a non-function return', async () => {
        const mountFn = vi.fn(() => 'not-a-cleanup');
        mountResolvesTo({ default: mountFn });
        const ctx = { slot: 'nav.page' as const, version: SLOT_CONTRACT_VERSION };
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
        const ctx = { slot: 'nav.page' as const, version: SLOT_CONTRACT_VERSION };
        await act(async () => render(withProviders(<MFEMount config={NAV} ctx={ctx} />)));
        expect(mountFn).toHaveBeenCalledTimes(1);
    });

    it('does not call a module that exposes no mount function', async () => {
        mountResolvesTo({ somethingElse: true });
        const ctx = { slot: 'nav.page' as const, version: SLOT_CONTRACT_VERSION };
        await act(async () => render(withProviders(<MFEMount config={NAV} ctx={ctx} />)));
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
        const ctx = { slot: 'nav.page' as const, version: SLOT_CONTRACT_VERSION };
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
        const ctx = { slot: 'nav.page' as const, version: SLOT_CONTRACT_VERSION };
        await act(async () => render(withProviders(<MFEMount config={disabled} ctx={ctx} />)));
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
            version: SLOT_CONTRACT_VERSION,
            entity: { urn: DATASET_URN, type: 'CHART' },
        });
    });

    it('renders the legacy (non-redesign) page chrome', async () => {
        const mountFn = vi.fn(() => vi.fn());
        getRemoteMock.mockResolvedValue({ mount: mountFn });
        unwrapModuleMock.mockResolvedValue({ mount: mountFn });
        await act(async () => {
            render(withProviders(<MFEBaseConfigurablePage config={NAV} />));
        });
        expect(screen.getByTestId('mfe-configurable-container')).toBeInTheDocument();
    });
});
