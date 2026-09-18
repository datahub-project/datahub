import { useMemo } from 'react';

import { MFEConfig, getPlacement, useMFEConfig } from '@app/mfeframework/mfeConfigLoader';
import { MFESlotId } from '@app/mfeframework/slots/slotTypes';

type ResolveSlotFilter = {
    /** GraphQL EntityType name of the page being rendered (e.g. `DATASET`). */
    entityType?: string;
};

export function matchesEntityType(config: MFEConfig, entityType?: string): boolean {
    const { entityTypes } = getPlacement(config);
    if (!entityTypes || entityTypes.length === 0) return true;
    if (!entityType) return false;
    const wanted = entityType.toLowerCase();
    return entityTypes.some((t) => t.toLowerCase() === wanted);
}

export function resolveSlot(configs: MFEConfig[], slot: MFESlotId, filter: ResolveSlotFilter = {}): MFEConfig[] {
    return configs.filter(
        (config) =>
            config.flags?.enabled && getPlacement(config).slot === slot && matchesEntityType(config, filter.entityType),
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
