import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';

import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { UserEntity } from '../../app/entity/user/User';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { EntityRegistryContext } from '../../entityRegistryContext';

type Props = {
    children: React.ReactNode;
};

export function getTestEntityRegistry() {
    const entityRegistry = new EntityRegistry();
    entityRegistry.register(new DatasetEntity());
    entityRegistry.register(new UserEntity());
    return entityRegistry;
}

export default ({ children }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);

    return (
        <MemoryRouter>
            <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>
        </MemoryRouter>
    );
};
