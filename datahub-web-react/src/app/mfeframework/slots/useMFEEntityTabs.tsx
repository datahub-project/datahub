import React, { useMemo } from 'react';

import { EntityTab } from '@app/entityV2/shared/types';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import MFEEntityTab from '@app/mfeframework/slots/MFEEntityTab';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

export function mfeConfigToEntityTab(config: MFEConfig, entityType: string): EntityTab {
    const iconName = config.navIcon;
    return {
        id: `mfe-${config.id}`,
        name: config.label,
        // Addressing, deliberately separate from presentation: the URL segment comes from the stable
        // `id`, so renaming — or translating — the caption never breaks an existing deep link.
        routeKey: `mfe-${config.id}`,
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

/**
 * Entity profile tabs contributed by MFEs placed in the `entity.detail.tab` slot for this entity type.
 */
export function useMFEEntityTabs(entityType: string): EntityTab[] {
    const configs = useResolveSlot('entity.detail.tab', { entityType });
    return useMemo(() => configs.map((config) => mfeConfigToEntityTab(config, entityType)), [configs, entityType]);
}
