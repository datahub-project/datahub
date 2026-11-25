import React from 'react';

import EntityRegistryV2 from '@app/entityV2/EntityRegistry';

export type EntityRegistry = EntityRegistryV2;

export const EntityRegistryContext = React.createContext<EntityRegistry>(new EntityRegistryV2());
