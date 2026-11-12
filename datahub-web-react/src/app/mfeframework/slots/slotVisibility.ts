import { isLogicalModel } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { GenericEntityProperties } from '@app/entityV2/shared/types';
import type { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { getPlacement } from '@app/mfeframework/slots/slotPlacement';

import { EntityType } from '@types';

/**
 * A named, host-registered visibility rule referenced from `placement.visibleWhen`.
 *
 * Predicates run on the HOST, before the MFE mounts, so a hidden slot never costs a module-federation
 * fetch. An MFE cannot remove its own tab, so any "should this even appear" rule has to live here.
 */
export type SlotVisibilityPredicate = (entityType: string, entityData: GenericEntityProperties | null) => boolean;

export const SLOT_VISIBILITY_PREDICATES: Record<string, SlotVisibilityPredicate> = {
    /**
     * A dataset backed by a real, physical asset — i.e. NOT a logical model.
     *
     * "Logical" is not an entity type or a subtype: a logical model is an ordinary dataset whose data
     * platform is flagged `logical: true`, so `placement.entityTypes: [dataset]` cannot tell the two
     * apart. That distinction is only visible here, where `entityData.platform.properties.logical`
     * has been loaded. Slots whose feature is meaningless without a physical asset (e.g. requesting
     * access to one) opt in with `visibleWhen: [physicalDataset]`.
     */
    physicalDataset: (entityType, entityData) =>
        entityType === (EntityType.Dataset as string) && !isLogicalModel(EntityType.Dataset, entityData),

    /**
     * A logical model: a dataset on a platform flagged `logical: true`, typically with physical
     * instances linked to it. Slots that only make sense on the logical model (e.g. listing its
     * physical instances) opt in with `visibleWhen: [logicalDataset]`.
     *
     * Unlike `physicalDataset`, this reports `false` while `entityData` is still loading, so the tab
     * appears once the platform flag is known rather than flashing on every dataset page.
     */
    logicalDataset: (entityType, entityData) =>
        entityType === (EntityType.Dataset as string) && isLogicalModel(EntityType.Dataset, entityData),

    /**
     * A physical dataset linked to a logical model (it has a `logicalParent`). Slots that compare a
     * physical asset with its model opt in with `visibleWhen: [physicalChild]`. Like `logicalDataset`, it
     * reports `false` until `entityData` has loaded, so the slot appears late rather than flashing.
     */
    physicalChild: (entityType, entityData) =>
        entityType === (EntityType.Dataset as string) &&
        !!entityData?.logicalParent &&
        !isLogicalModel(EntityType.Dataset, entityData),
};

/**
 * Layer 2 of slot-tab visibility: evaluates the entry's `visibleWhen` allow-list against the entity
 * currently on screen. (Layer 1, the coarse `entityTypes` page-class filter, already ran in
 * `resolveSlot` before any `EntityTab` existed.)
 *
 * - no/empty `visibleWhen` => visible (an entry that declares no rule is not constrained by one)
 * - visible if ANY named predicate matches (logical OR)
 * - an unknown predicate name contributes no match and logs — so a typo hides the tab rather than
 *   silently showing it somewhere it was meant to be excluded from
 *
 * Note: while the page is still loading, `entityData` is `null` and a predicate may report `true`
 * (e.g. `physicalDataset` cannot yet see the platform flag). The tab can therefore appear for a beat
 * and then hide once the data lands (`logicalDataset` reports `false` instead, so it appears late). This matches how DataHub's own `display.visible` gates behave
 * with in-flight `entityData`, and it self-corrects on the next render.
 */
export function isSlotTabVisible(
    config: MFEConfig,
    entityType: string,
    entityData: GenericEntityProperties | null,
): boolean {
    const rules = getPlacement(config).visibleWhen;
    if (!rules || rules.length === 0) {
        return true;
    }
    return rules.some((name) => {
        const predicate = SLOT_VISIBILITY_PREDICATES[name];
        if (!predicate) {
            console.error(
                `[MFE Slot] ${config.id}: unknown placement.visibleWhen predicate "${name}"; ignoring it. ` +
                    `Known predicates: ${Object.keys(SLOT_VISIBILITY_PREDICATES).join(', ')}`,
            );
            return false;
        }
        return predicate(entityType, entityData);
    });
}
