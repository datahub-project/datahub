import { useContext } from 'react';

import { EntityRegistryContext } from '@src/entityRegistryContext';

/**
 * Fetch an instance of EntityRegistry from the React context.
 */
export function useEntityRegistry() {
    return useContext(EntityRegistryContext);
}
