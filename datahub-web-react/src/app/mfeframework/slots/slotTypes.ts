/**
 * Typed slot contract between the DataHub host and a micro frontend (MFE).
 *
 * A *slot* is a named, host-owned region an MFE can be placed into via the `placement` field of its
 * `microFrontends[]` YAML entry. The host builds a typed context for the slot at render time and hands
 * it to the MFE's `mount(el, ctx)` entry point. MFE authors should `import type` from this file so both
 * sides of the boundary share one definition.
 *
 * Versioning: an entry declares the shape it was built against via `placement.contractVersion`, and the
 * host stamps that version back into the context it hands over. MFEs must ignore fields they do not
 * recognise, so a new OPTIONAL field can be added to a shipped version without a new version — mark it
 * optional and note which release began sending it, since older hosts will omit it. Add a NEW versioned
 * type here plus a builder in `slotContextBuilders` only when a change can break a reader (removing or
 * renaming a field, changing its type or its meaning) or when an MFE must be able to *require* something
 * newly added. Never widen `SlotBaseContext` with surface-specific fields.
 */

/** Slots the host currently exposes. `nav.page` is the original full-page MFE surface. */
export type MFESlotId = 'nav.page' | 'entity.detail.tab';

export const MFE_SLOT_IDS: readonly MFESlotId[] = ['nav.page', 'entity.detail.tab'];

export const DEFAULT_MFE_SLOT: MFESlotId = 'nav.page';

/** The contract version assumed for entries that predate `placement` (legacy nav-only YAML). */
export const DEFAULT_SLOT_CONTRACT_VERSION = '1.0.0';

/** Fields every slot context carries, regardless of surface. */
export type SlotBaseContext = {
    slot: MFESlotId;
    /** Host-stamped at render time; mirrors the entry's `placement.contractVersion`. */
    contractVersion: string;
    /** The authenticated user, when known. MFEs can also call GraphQL directly with the session. */
    principal?: { user: string };
};

/** Context for a full-page MFE reached from the left navigation (`/mfe<path>`) — v1. */
export type NavPageContextV1 = SlotBaseContext & {
    slot: 'nav.page';
    contractVersion: '1.0.0';
};

/** Context for an MFE rendered as a tab on an entity profile page — v1. */
export type EntityDetailTabContextV1 = SlotBaseContext & {
    slot: 'entity.detail.tab';
    contractVersion: '1.0.0';
    entity: {
        urn: string;
        /** GraphQL `EntityType` name, e.g. `DATASET`. */
        type: string;
    };
};

/**
 * Every versioned context the host can emit for a slot. Widen these unions as new versions ship
 * (e.g. `NavPageContextV1 | NavPageContextV2`). Adding an optional field to a shipped version is fine;
 * removing one, or changing its type or meaning, needs a new version.
 */
export type NavPageContext = NavPageContextV1;
export type EntityDetailTabContext = EntityDetailTabContextV1;

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
