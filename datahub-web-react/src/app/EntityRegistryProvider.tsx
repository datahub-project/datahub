import React from 'react';

import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import EntityRegistry from '@app/entity/EntityRegistry';
import useBuildEntityRegistry from '@app/useBuildEntityRegistry';
import { EntityRegistryContext } from '@src/entityRegistryContext';

export const globalEntityRegistryV2 = buildEntityRegistryV2();

const EntityRegistryProvider = ({ children }: { children: React.ReactNode }) => {
    const entityRegistry = useBuildEntityRegistry() as EntityRegistry;
    return <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>;
};

export default EntityRegistryProvider;
