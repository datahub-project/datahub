import React, { useMemo } from 'react';

import { EntityTab } from '@app/entityV2/shared/types';
import { getLazyIcon } from '@app/mfeframework/lazyIconRegistry';
import { MFEConfig, getPlacement } from '@app/mfeframework/mfeConfigLoader';
import MFEEntityTab from '@app/mfeframework/slots/MFEEntityTab';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

export function mfeConfigToEntityTab(config: MFEConfig): EntityTab {
    const placement = getPlacement(config);
    const iconName = config.navIcon;
    return {
        id: `mfe-${config.id}`,
        name: placement.tabName ?? config.label,
        icon: iconName ? () => getLazyIcon(iconName) : undefined,
        component: () => <MFEEntityTab config={config} />,
        display: {
            visible: () => true,
            enabled: () => true,
        },
    };
}

/**
 * Entity profile tabs contributed by MFEs placed in the `entity.detail.tab` slot for this entity type.
 */
export function useMFEEntityTabs(entityType: string): EntityTab[] {
    const configs = useResolveSlot('entity.detail.tab', { entityType });
    return useMemo(() => configs.map(mfeConfigToEntityTab), [configs]);
}
