import React from 'react';

import type EntityRegistryV2 from '@app/entityV2/EntityRegistry';

export type EntityRegistry = EntityRegistryV2;

// No default instance: entityV2/EntityRegistry imports this module back (via lineageUtils ->
// useEntityRegistry), so constructing one here can hit the class before it is initialized.
// The app, tests and stories all render inside an EntityRegistryContext provider.
export const EntityRegistryContext = React.createContext<EntityRegistry>(undefined as unknown as EntityRegistry);
