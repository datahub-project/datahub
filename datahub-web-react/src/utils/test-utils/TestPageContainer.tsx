import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { UserEntity } from '../../app/entity/user/User';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { EntityRegistryContext } from '../../entityRegistryContext';
import { TagEntity } from '../../app/entity/tag/Tag';

type Props = {
    children: React.ReactNode;
    initialEntries?: string[];
};

export function getTestEntityRegistry() {
    const entityRegistry = new EntityRegistry();
    entityRegistry.register(new DatasetEntity());
    entityRegistry.register(new UserEntity());
    entityRegistry.register(new TagEntity());
    return entityRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);
    return (
        <MemoryRouter initialEntries={initialEntries}>
            <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>
        </MemoryRouter>
    );
};
