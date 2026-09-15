import React from 'react';

import globalEntityRegistryV2 from '@app/globalEntityRegistryV2';
import { EntityRegistryContext } from '@src/entityRegistryContext';

const EntityRegistryProvider = ({ children }: { children: React.ReactNode }) => {
    return <EntityRegistryContext.Provider value={globalEntityRegistryV2}>{children}</EntityRegistryContext.Provider>;
};

export default EntityRegistryProvider;
