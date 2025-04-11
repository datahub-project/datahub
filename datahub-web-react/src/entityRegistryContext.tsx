import React from 'react';
import EntityRegistryV1 from './app/entity/EntityRegistry';
import EntityRegistryV2 from './app/entityV2/EntityRegistry';

export type EntityRegistry = EntityRegistryV1 | EntityRegistryV2;

export const EntityRegistryContext = React.createContext<EntityRegistryV1>(new EntityRegistryV1());
