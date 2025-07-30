import { useContext } from 'react';

import EntityRegistryV2 from '@app/entityV2/EntityRegistry';
import { EntityRegistryContext } from '@src/entityRegistryContext';

/**
 * Fetch an instance of EntityRegistry from the React context.
 */
export function useEntityRegistry() {
    return useContext(EntityRegistryContext);
}

export function useEntityRegistryV2() {
    return useContext(EntityRegistryContext) as any as EntityRegistryV2;
}
