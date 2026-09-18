/**
 * Typed slot contract between the DataHub host and a micro frontend (MFE).
 *
 * A *slot* is a named, host-owned region an MFE can be placed into via the `placement` field of its
 * `microFrontends[]` YAML entry. The host builds a typed context for the slot at render time and hands
 * it to the MFE's `mount(el, ctx)` entry point. MFE authors should `import type` from this file so both
 * sides of the boundary share one definition.
 *
 * Versioning: to pass more context to a slot, extend that slot's context type and bump
 * `SLOT_CONTRACT_VERSION`. Never widen `SlotBaseContext` with surface-specific fields.
 */

/** Slots the host currently exposes. `nav.page` is the original full-page MFE surface. */
export type MFESlotId = 'nav.page' | 'entity.detail.tab';

export const MFE_SLOT_IDS: readonly MFESlotId[] = ['nav.page', 'entity.detail.tab'];

export const DEFAULT_MFE_SLOT: MFESlotId = 'nav.page';

export const SLOT_CONTRACT_VERSION = '1.0.0';

/** Fields every slot context carries, regardless of surface. */
export type SlotBaseContext = {
    slot: MFESlotId;
    version: string;
    /** The authenticated user, when known. MFEs can also call GraphQL directly with the session. */
    principal?: { user: string };
};

/** Context for a full-page MFE reached from the left navigation (`/mfe<path>`). */
export type NavPageContext = SlotBaseContext & {
    slot: 'nav.page';
};

/** Context for an MFE rendered as a tab on an entity profile page. */
export type EntityDetailTabContext = SlotBaseContext & {
    slot: 'entity.detail.tab';
    entity: {
        urn: string;
        /** GraphQL `EntityType` name, e.g. `DATASET`. */
        type: string;
    };
};

export type SlotContextMap = {
    'nav.page': NavPageContext;
    'entity.detail.tab': EntityDetailTabContext;
};

export type SlotContext = SlotContextMap[MFESlotId];

/** Signature of the `mount` function an MFE exposes for a given slot. */
export type MFEMountFn<S extends MFESlotId = MFESlotId> = (
    el: HTMLElement,
    ctx: SlotContextMap[S],
) => (() => void) | void;

export function isMFESlotId(value: unknown): value is MFESlotId {
    return typeof value === 'string' && (MFE_SLOT_IDS as readonly string[]).includes(value);
}
