import React from 'react';

import type EntityRegistryV2 from '@app/entityV2/EntityRegistry';

export type EntityRegistry = EntityRegistryV2;

// No default instance: EntityRegistry imports this module back, so creating one here can crash at load.
export const EntityRegistryContext = React.createContext<EntityRegistry>(undefined as unknown as EntityRegistry);
