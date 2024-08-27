import React from 'react';
import { EntityRegistryContext } from '../entityRegistryContext';
import useBuildEntityRegistry from './useBuildEntityRegistry';

const EntityRegistryProvider = ({ children }: { children: React.ReactNode }) => {
    const entityRegistry = useBuildEntityRegistry();
    return <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>;
};

export default EntityRegistryProvider;
