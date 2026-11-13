import React, { useMemo } from 'react';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { EntitySidebarSection } from '@app/entityV2/shared/types';
import { MFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { RegionEntry } from '@app/mfeframework/slots/MFEEntityRegion';
import { insertAfterAnchors } from '@app/mfeframework/slots/insertAfterAnchors';
import { isSlotTabVisible } from '@app/mfeframework/slots/slotVisibility';
import { useResolveSlot } from '@app/mfeframework/slots/useResolveSlot';

export function mfeConfigToSidebarSection(config: MFEConfig, entityType: string): EntitySidebarSection {
    const component = () => <RegionEntry config={config} />;
    // The sidebar keys its children by component name; give each placed MFE its own.
    component.displayName = `MFESidebarSection(${config.id})`;
    return {
        key: `mfe-${config.id}`,
        component,
        display: {
            // Same two visibility layers as slot tabs: entityTypes already ran in resolveSlot, visibleWhen here.
            visible: (entityData) => isSlotTabVisible(config, entityType, entityData),
        },
    };
}

/**
 * The entity's sidebar sections with those contributed by MFEs placed in the `entity.sidebar.section` slot
 * merged in, each right after the section whose `key` its `placement.after` names (else at the end).
 */
export function useMFESidebarSections(hostSections: EntitySidebarSection[]): EntitySidebarSection[] {
    const { entityType } = useEntityData();
    const configs = useResolveSlot('entity.sidebar.section', { entityType });
    // Built once per config so each section's component keeps its identity: a new one would remount the remote.
    const slotSections = useMemo(
        () =>
            configs.map((entry) => ({
                item: mfeConfigToSidebarSection(entry, entityType),
                after: entry.placement?.after,
            })),
        [configs, entityType],
    );
    return insertAfterAnchors(hostSections, slotSections, (section) => section.key);
}
