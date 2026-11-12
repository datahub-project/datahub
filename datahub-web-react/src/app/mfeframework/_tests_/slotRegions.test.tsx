import { act, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { UserContext } from '@app/context/userContext';
import { EntityContext } from '@app/entityV2/shared/EntityContext';
import { EntitySidebarSections } from '@app/entityV2/shared/containers/profile/sidebar/EntitySidebar';
import { EntitySidebarSection, TabContextType } from '@app/entityV2/shared/types';
import { MFEConfigContext } from '@app/mfeframework/MFEConfigContext';
import { MFEConfig, MFESchema } from '@app/mfeframework/mfeConfigLoader';
import { RegionEntry } from '@app/mfeframework/slots/MFEEntityRegion';
import MFEFieldDrawerSections from '@app/mfeframework/slots/MFEFieldDrawerSections';
import { buildSlotContext } from '@app/mfeframework/slots/slotContextBuilders';
import { MFESlotId } from '@app/mfeframework/slots/slotTypes';

import { EntityType } from '@types';

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

// The visibility registry reaches `logicalModels.utils`, which imports an enum from a React component module.
vi.mock('@app/entityV2/shared/EntityDropdown/EntityMenuActions', () => ({ EntityMenuItems: {} }));

const THEME = { styles: {}, colors: { bg: 'none', border: 'none' }, assets: {}, content: {} };
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)';
const USER = 'urn:li:corpuser:jdoe';

const entry = (id: string, slot: MFESlotId, extra: Record<string, unknown> = {}): MFEConfig => ({
    id,
    label: id,
    remoteEntry: 'http://localhost:3002/remoteEntry.js',
    module: 'demoMFE/mount',
    flags: { enabled: true },
    placement: { slot, contractVersion: '1.0.0', entityTypes: ['dataset'], ...extra },
});

function withPage(ui: React.ReactElement, microFrontends: MFEConfig[], { urn = DATASET_URN as string | null } = {}) {
    const schema: MFESchema = { topLevelMenuTitle: 'Apps', subNavigationMode: false, microFrontends };
    return (
        <MemoryRouter>
            <ThemeProvider theme={THEME as any}>
                <UserContext.Provider value={{ urn: USER } as any}>
                    <MFEConfigContext.Provider value={{ provided: true, config: schema, loading: false }}>
                        <EntityContext.Provider
                            value={{ urn, entityType: EntityType.Dataset, entityData: { platform: {} }, loading: false } as any}
                        >
                            {ui}
                        </EntityContext.Provider>
                    </MFEConfigContext.Provider>
                </UserContext.Provider>
            </ThemeProvider>
        </MemoryRouter>
    );
}

function mountResolves() {
    const mountFn = vi.fn(() => vi.fn());
    getRemoteMock.mockResolvedValue({ mount: mountFn });
    unwrapModuleMock.mockResolvedValue({ mount: mountFn });
    return mountFn;
}

beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => vi.restoreAllMocks());

describe('region and drawer context builders', () => {
    it('builds the region context without a principal and refuses one without an entity', () => {
        const sidebar = entry('side', 'entity.sidebar.section');
        expect(buildSlotContext(sidebar, { urn: DATASET_URN, entityType: 'DATASET' })).toEqual({
            slot: 'entity.sidebar.section',
            contractVersion: '1.0.0',
            entity: { urn: DATASET_URN, type: 'DATASET' },
        });
        expect(buildSlotContext(sidebar, { entityType: 'DATASET' })).toBeNull();
    });

    it('builds the column drawer context with the field, and refuses one without a field', () => {
        const drawer = entry('drawer', 'schemaField.drawer.section');
        const args = { urn: DATASET_URN, entityType: 'DATASET', fieldPath: 'account_id' };
        expect(buildSlotContext(drawer, { ...args, principal: { user: USER } })).toEqual({
            slot: 'schemaField.drawer.section',
            contractVersion: '1.0.0',
            entity: { urn: DATASET_URN, type: 'DATASET' },
            field: { fieldPath: 'account_id' },
            principal: { user: USER },
        });
        expect(buildSlotContext(drawer, args)).not.toHaveProperty('principal');
        expect(buildSlotContext(drawer, { ...args, fieldPath: '' })).toBeNull();
    });
});

describe('RegionEntry', () => {
    it('does not mount the remote before the page knows its entity', async () => {
        const mountFn = mountResolves();
        await act(async () => {
            render(withPage(<RegionEntry config={entry('top', 'entity.summary.top')} />, [], { urn: null }));
        });
        expect(mountFn).not.toHaveBeenCalled();
        expect(screen.queryByTestId('mfe-slot-container')).toBeNull();
    });
});

describe('MFEFieldDrawerSections', () => {
    it('mounts each entry placed in the column drawer with the column in its context', async () => {
        const mountFn = mountResolves();
        await act(async () => {
            render(
                withPage(<MFEFieldDrawerSections fieldPath="account_id" />, [
                    entry('drawer', 'schemaField.drawer.section'),
                    entry('side', 'entity.sidebar.section'),
                ]),
            );
        });
        expect(mountFn).toHaveBeenCalledTimes(1);
        expect(mountFn).toHaveBeenCalledWith(expect.anything(), {
            slot: 'schemaField.drawer.section',
            contractVersion: '1.0.0',
            entity: { urn: DATASET_URN, type: 'DATASET' },
            field: { fieldPath: 'account_id' },
            principal: { user: USER },
        });
    });

    it('renders nothing when no entry applies, and mounts nothing without a column', async () => {
        const mountFn = mountResolves();
        const { container } = render(withPage(<MFEFieldDrawerSections fieldPath="account_id" />, []));
        expect(container).toBeEmptyDOMElement();
        await act(async () => {
            render(withPage(<MFEFieldDrawerSections fieldPath="" />, [entry('drawer', 'schemaField.drawer.section')]));
        });
        expect(mountFn).not.toHaveBeenCalled();
    });
});

describe('EntitySidebarSections', () => {
    const hostSection = (key: string): EntitySidebarSection => {
        const component = () => <div data-testid={`host-${key}`} />;
        component.displayName = `Host(${key})`;
        return { key, component, display: { visible: () => true } };
    };

    it('renders a placed MFE section between the host sections it is placed after and before', async () => {
        mountResolves();
        await act(async () => {
            render(
                withPage(
                    <EntitySidebarSections
                        sidebarSections={[hostSection('notes'), hostSection('owners')]}
                        contextType={TabContextType.PROFILE_SIDEBAR}
                    />,
                    [entry('custom', 'entity.sidebar.section', { after: 'notes' })],
                ),
            );
        });
        const order = Array.from(
            document.querySelectorAll('[data-testid^="host-"], [data-testid="mfe-slot-container"]'),
        ).map((el) => el.getAttribute('data-testid'));
        expect(order).toEqual(['host-notes', 'mfe-slot-container', 'host-owners']);
    });

    it('still renders placed sections when the entity defines no sidebar sections', async () => {
        mountResolves();
        await act(async () => {
            render(
                withPage(
                    <EntitySidebarSections
                        sidebarSections={undefined as unknown as EntitySidebarSection[]}
                        contextType={TabContextType.PROFILE_SIDEBAR}
                    />,
                    [entry('custom', 'entity.sidebar.section')],
                ),
            );
        });
        expect(screen.getByTestId('mfe-slot-container')).toBeInTheDocument();
    });
});
