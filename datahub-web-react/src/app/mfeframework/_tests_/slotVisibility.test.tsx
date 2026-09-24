import { beforeEach, describe, expect, it, vi } from 'vitest';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { buildSlotContext, isSlotContractSupported } from '@app/mfeframework/slots/slotContextBuilders';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';

import { EntityType } from '@types';

// `logicalModels.utils` imports an enum from EntityMenuActions — a React component module — purely for
// an unrelated menu helper. Stub it so these tests exercise the real `isLogicalModel` without pulling
// the entity-dropdown UI tree (and its heavy transitive deps) into the test graph.
vi.mock('@app/entityV2/shared/EntityDropdown/EntityMenuActions', () => ({ EntityMenuItems: {} }));

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)';

function tabConfig(visibleWhen?: string[], contractVersion = '1.0.0'): MFEConfig {
    return {
        id: 'access-tab',
        label: 'Access',
        remoteEntry: 'http://localhost:3002/remoteEntry.js',
        module: 'accessMFE/mount',
        flags: { enabled: true },
        placement: { slot: 'entity.detail.tab', contractVersion, entityTypes: ['dataset'], visibleWhen },
    };
}

const LEGACY_NAV: MFEConfig = {
    id: 'nav-app',
    label: 'Nav App',
    path: '/nav-app',
    remoteEntry: 'http://localhost:3002/remoteEntry.js',
    module: 'navApp/mount',
    flags: { enabled: true, showInNav: true },
    navIcon: 'Globe',
};

/** A dataset on a platform flagged `logical: true` — a hand-authored model, no physical asset. */
const LOGICAL_DATASET = { platform: { properties: { logical: true } } } as GenericEntityProperties;
const PHYSICAL_DATASET = { platform: { properties: { logical: false } } } as GenericEntityProperties;

describe('slot context builders', () => {
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    it('builds the entity.detail.tab context for the declared version, stamping slot and version', () => {
        const ctx = buildSlotContext(tabConfig(), {
            urn: DATASET_URN,
            entityType: 'DATASET',
            principal: { user: 'urn:li:corpuser:jdoe' },
        });
        expect(ctx).toEqual({
            slot: 'entity.detail.tab',
            contractVersion: '1.0.0',
            entity: { urn: DATASET_URN, type: 'DATASET' },
            principal: { user: 'urn:li:corpuser:jdoe' },
        });
    });

    it('omits principal when the viewer is unknown', () => {
        const ctx = buildSlotContext(tabConfig(), { urn: DATASET_URN, entityType: 'DATASET' });
        expect(ctx).not.toHaveProperty('principal');
    });

    it('builds a nav.page context for a legacy entry that declares no placement', () => {
        expect(isSlotContractSupported(LEGACY_NAV)).toBe(true);
        expect(buildSlotContext(LEGACY_NAV, { principal: { user: 'urn:li:corpuser:jdoe' } })).toEqual({
            slot: 'nav.page',
            contractVersion: '1.0.0',
            principal: { user: 'urn:li:corpuser:jdoe' },
        });
    });

    // Gate 2 — fail closed rather than hand a remote a shape it was not built for.
    it('returns null and logs for a contract version the host cannot build', () => {
        const future = tabConfig(undefined, '2.0.0');
        expect(isSlotContractSupported(future)).toBe(false);
        expect(buildSlotContext(future, { urn: DATASET_URN, entityType: 'DATASET' })).toBeNull();
        expect(console.error).toHaveBeenCalledWith(expect.stringContaining('2.0.0'));
    });
});

describe('slot visibility (Layer 2)', () => {
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    it('shows the tab when no visibleWhen is declared', () => {
        expect(isSlotTabVisible(tabConfig(), EntityType.Dataset, LOGICAL_DATASET)).toBe(true);
        expect(isSlotTabVisible(tabConfig([]), EntityType.Dataset, LOGICAL_DATASET)).toBe(true);
    });

    it('shows a physicalDataset tab on a physical dataset', () => {
        expect(isSlotTabVisible(tabConfig(['physicalDataset']), EntityType.Dataset, PHYSICAL_DATASET)).toBe(true);
    });

    // The reason Layer 2 exists: a logical model is an ordinary dataset, so entityTypes cannot exclude it.
    it('hides a physicalDataset tab on a logical dataset', () => {
        expect(isSlotTabVisible(tabConfig(['physicalDataset']), EntityType.Dataset, LOGICAL_DATASET)).toBe(false);
    });

    it('hides a physicalDataset tab on a non-dataset entity', () => {
        expect(isSlotTabVisible(tabConfig(['physicalDataset']), EntityType.Chart, null)).toBe(false);
    });

    it('is visible while entityData is still loading, then self-corrects', () => {
        expect(isSlotTabVisible(tabConfig(['physicalDataset']), EntityType.Dataset, null)).toBe(true);
    });

    it('treats an unknown predicate as no match and logs', () => {
        expect(isSlotTabVisible(tabConfig(['nopeNotReal']), EntityType.Dataset, PHYSICAL_DATASET)).toBe(false);
        expect(console.error).toHaveBeenCalledWith(expect.stringContaining('nopeNotReal'));
    });

    it('is visible if ANY listed predicate matches', () => {
        expect(
            isSlotTabVisible(tabConfig(['nopeNotReal', 'physicalDataset']), EntityType.Dataset, PHYSICAL_DATASET),
        ).toBe(true);
    });
});
