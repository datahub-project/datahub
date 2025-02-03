import { useContext } from 'react';
import { EntityRegistryContext } from '../entityRegistryContext';
import EntityRegistryV2 from './entityV2/EntityRegistry';

/**
 * Fetch an instance of EntityRegistry from the React context.
 */
export function useEntityRegistry() {
    return useContext(EntityRegistryContext);
}

export function useEntityRegistryV2() {
    return useContext(EntityRegistryContext) as any as EntityRegistryV2;
}
