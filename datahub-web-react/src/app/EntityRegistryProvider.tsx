import buildEntityRegistry from '@app/buildEntityRegistry';
import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import React from 'react';
import { EntityRegistryContext } from '../entityRegistryContext';
import EntityRegistry from './entity/EntityRegistry';
import useBuildEntityRegistry from './useBuildEntityRegistry';

export const globalEntityRegistry = buildEntityRegistry();
export const globalEntityRegistryV2 = buildEntityRegistryV2();

const EntityRegistryProvider = ({ children }: { children: React.ReactNode }) => {
    const entityRegistry = useBuildEntityRegistry() as EntityRegistry;
    return <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>;
};

export default EntityRegistryProvider;
