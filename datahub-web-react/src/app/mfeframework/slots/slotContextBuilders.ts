import type { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { getPlacement } from '@app/mfeframework/slots/slotPlacement';
import { EntityDetailTabContextV1, MFESlotId, NavPageContextV1, SlotContext } from '@app/mfeframework/slots/slotTypes';

/**
 * Everything the host can offer a slot context builder. Each builder takes only what its slot's
 * contract version actually declares and ignores the rest.
 */
export type SlotContextBuilderArgs = {
    /** URN of the entity the page is showing (entity-scoped slots only). */
    urn?: string;
    /** GraphQL `EntityType` name of the page (entity-scoped slots only). */
    entityType?: string;
    /** The authenticated viewer, when known. */
    principal?: { user: string };
};

export type SlotContextBuilder = (args: SlotContextBuilderArgs) => SlotContext;

function buildNavPageContextV1({ principal }: SlotContextBuilderArgs): NavPageContextV1 {
    return {
        slot: 'nav.page',
        contractVersion: '1.0.0',
        ...(principal ? { principal } : {}),
    };
}

function buildEntityDetailTabContextV1({
    urn,
    entityType,
    principal,
}: SlotContextBuilderArgs): EntityDetailTabContextV1 {
    return {
        slot: 'entity.detail.tab',
        contractVersion: '1.0.0',
        entity: { urn: urn ?? '', type: entityType ?? '' },
        ...(principal ? { principal } : {}),
    };
}

/**
 * The context shapes this host can produce, keyed by `placement.contractVersion` then by slot.
 *
 * Adding a version is purely additive: add the new versioned type in `slotTypes`, add a builder here,
 * and register it under its version key. A released key may gain an optional field — MFEs ignore what
 * they do not recognise — but never loses one or changes what one means, so an MFE built against it
 * keeps receiving what it expects.
 */
export const SLOT_CONTEXT_BUILDERS: Record<string, Partial<Record<MFESlotId, SlotContextBuilder>>> = {
    '1.0.0': {
        'nav.page': buildNavPageContextV1,
        'entity.detail.tab': buildEntityDetailTabContextV1,
    },
};

/**
 * Gate 2 — resolves the builder for an entry's declared slot + contract version.
 *
 * Returns `null` (and logs) when the entry declares a version this host has no builder for, which is
 * the fail-closed posture: the caller must not render the MFE rather than hand it a context shape it
 * was not built for. Gate 1 (`validatePlacement`) already guarantees `contractVersion` is present and
 * non-empty by the time we get here.
 */
export function getSlotContextBuilder(config: MFEConfig): SlotContextBuilder | null {
    const { slot, contractVersion } = getPlacement(config);
    const builder = SLOT_CONTEXT_BUILDERS[contractVersion]?.[slot];
    if (!builder) {
        console.error(
            `[MFE Slot] ${config.id}: no context builder for slot "${slot}" at placement.contractVersion ` +
                `"${contractVersion}"; not rendering it. Known versions: ${Object.keys(SLOT_CONTEXT_BUILDERS).join(
                    ', ',
                )}`,
        );
        return null;
    }
    return builder;
}

/** Whether this host can build a context for the entry — used to drop unsupported entries early. */
export function isSlotContractSupported(config: MFEConfig): boolean {
    return getSlotContextBuilder(config) !== null;
}

/** Builds the typed context for an entry, or `null` when its contract version is unsupported. */
export function buildSlotContext(config: MFEConfig, args: SlotContextBuilderArgs = {}): SlotContext | null {
    const builder = getSlotContextBuilder(config);
    return builder ? builder(args) : null;
}
