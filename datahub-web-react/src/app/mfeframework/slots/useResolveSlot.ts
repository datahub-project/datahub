import { useMemo } from 'react';

import { MFEConfig, useMFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { isSlotContractSupported } from '@app/mfeframework/slots/slotContextBuilders';
import { getPlacement } from '@app/mfeframework/slots/slotPlacement';
import { MFESlotId } from '@app/mfeframework/slots/slotTypes';

type ResolveSlotFilter = {
    /** GraphQL EntityType name of the page being rendered (e.g. `DATASET`). */
    entityType?: string;
};

/** Slots that render on an entity page, and therefore must declare which entity types they apply to. */
const ENTITY_SCOPED_SLOTS: readonly MFESlotId[] = ['entity.detail.tab'];

/**
 * Layer 1 of visibility: the coarse page-class gate, evaluated before an `EntityTab` is created and
 * before any remote JS loads.
 *
 * Fail closed — an entry that does not list `entityTypes` matches nothing. Placing an MFE onto every
 * entity page in the catalog is never the safe default to infer from an omission, so appearing on a
 * page is an explicit opt-in.
 */
export function matchesEntityType(config: MFEConfig, entityType?: string): boolean {
    const { entityTypes } = getPlacement(config);
    if (!entityTypes || entityTypes.length === 0) return false;
    if (!entityType) return false;
    const wanted = entityType.toLowerCase();
    return entityTypes.some((t) => t.toLowerCase() === wanted);
}

export function resolveSlot(configs: MFEConfig[], slot: MFESlotId, filter: ResolveSlotFilter = {}): MFEConfig[] {
    const isEntityScoped = ENTITY_SCOPED_SLOTS.includes(slot);
    return configs.filter(
        (config) =>
            config.flags?.enabled &&
            getPlacement(config).slot === slot &&
            // Gate 2 — an entry declaring a contract version this host cannot build is dropped here, so
            // the tab never appears and the remote is never fetched.
            isSlotContractSupported(config) &&
            (!isEntityScoped || matchesEntityType(config, filter.entityType)),
    );
}

/**
 * The enabled MFEs placed in `slot`, from the shared YAML config. YAML is the registry: there is no
 * runtime `registerSlot`, so the host knows which tabs exist without loading any remote code.
 */
export function useResolveSlot(slot: MFESlotId, filter: ResolveSlotFilter = {}): MFEConfig[] {
    const { config } = useMFEConfig();
    const { entityType } = filter;
    return useMemo(() => resolveSlot(config?.microFrontends ?? [], slot, { entityType }), [config, slot, entityType]);
}
