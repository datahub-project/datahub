import React, { useMemo } from 'react';

import { EntityTab } from '@app/entityV2/shared/types';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import MFEEntityTab from '@app/mfeframework/slots/MFEEntityTab';
import { insertAfterAnchors } from '@app/mfeframework/slots/insertAfterAnchors';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

const NO_TABS: EntityTab[] = [];

export function mfeConfigToEntityTab(config: MFEConfig, entityType: string): EntityTab {
    const iconName = config.navIcon;
    return {
        id: `mfe-${config.id}`,
        name: config.label,
        // Addressed in the URL by the stable entry id, so rewording the caption never breaks a deep link.
        path: `mfe-${config.id}`,
        icon: iconName ? () => getLazyIcon(iconName) : undefined,
        component: () => <MFEEntityTab config={config} />,
        display: {
            // Layer 1 (entityTypes) already passed in resolveSlot. Layer 2 runs here, in the host,
            // before the tab is rendered — so a hidden tab never fetches the remote bundle.
            visible: (entityData) => isSlotTabVisible(config, entityType, entityData),
            enabled: () => true,
        },
    };
}

/** Places each slot tab right after the host tab whose `path` its `placement.after` names (see insertAfterAnchors). */
export function insertSlotTabs(hostTabs: EntityTab[], slotTabs: { tab: EntityTab; after?: string }[]): EntityTab[] {
    return insertAfterAnchors(
        hostTabs,
        slotTabs.map(({ tab, after }) => ({ item: tab, after })),
        (tab) => tab.path,
    );
}

/**
 * The entity profile's tabs with those contributed by MFEs placed in the `entity.detail.tab` slot for this
 * entity type merged in at their configured positions. With no `hostTabs`, just the slot tabs.
 */
export function useMFEEntityTabs(entityType: string, hostTabs: EntityTab[] = NO_TABS): EntityTab[] {
    const configs = useResolveSlot('entity.detail.tab', { entityType });
    // Built once per config so each tab's `component` keeps its identity across renders; a new function
    // there would remount the remote on every parent render.
    const slotTabs = useMemo(
        () =>
            configs.map((config) => ({
                tab: mfeConfigToEntityTab(config, entityType),
                after: config.placement?.after,
            })),
        [configs, entityType],
    );
    return insertSlotTabs(hostTabs, slotTabs);
}
