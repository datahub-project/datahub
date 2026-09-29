import type { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { DEFAULT_MFE_SLOT, DEFAULT_SLOT_CONTRACT_VERSION, MFESlotId } from '@app/mfeframework/slots/slotTypes';

/**
 * Where an MFE renders. Omitted => `nav.page` (a full page reached from the left navigation),
 * which is how every entry behaved before placements existed.
 *
 * This lives outside `mfeConfigLoader` so the slot modules (context builders, visibility) can read a
 * placement without importing the loader at runtime — the loader renders `MFEBaseConfigurablePage`,
 * which would otherwise close an import cycle.
 */
export type MFEPlacement = {
    slot: MFESlotId;
    /**
     * REQUIRED whenever `placement` is present. Declares which context shape the MFE was built
     * against; the host picks the matching context builder at render time. Entries that declare a
     * version the host has no builder for are not rendered (fail closed).
     */
    contractVersion: string;
    /**
     * Coarse page-class filter for entity-scoped slots: GraphQL EntityType names (case-insensitive,
     * e.g. `dataset`). Omitted => the placement matches no entity type, so the MFE is shown nowhere.
     * Placement into an entity-scoped slot is an explicit opt-in.
     */
    entityTypes?: string[];
    /**
     * Fine-grained visibility allow-list: names of host-registered predicates (see `slotVisibility`).
     * Visible if ANY predicate matches. Omitted/empty => no extra constraint.
     */
    visibleWhen?: string[];
};

/**
 * The effective placement for an entry. Entries written before `placement` existed are treated as
 * `nav.page` on the original contract version, so legacy YAML keeps working untouched.
 */
export function getPlacement(config: MFEConfig): MFEPlacement {
    return config.placement ?? { slot: DEFAULT_MFE_SLOT, contractVersion: DEFAULT_SLOT_CONTRACT_VERSION };
}

export function isNavPageMfe(config: MFEConfig): boolean {
    return getPlacement(config).slot === 'nav.page';
}
