import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';

import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { DataFlowEntity } from '../../app/entity/dataFlow/DataFlowEntity';
import { DataJobEntity } from '../../app/entity/dataJob/DataJobEntity';
import { UserEntity } from '../../app/entity/user/User';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { EntityRegistryContext } from '../../entityRegistryContext';
import { TagEntity } from '../../app/entity/tag/Tag';

import defaultThemeConfig from '../../conf/theme/theme_light.config.json';

type Props = {
    children: React.ReactNode;
    initialEntries?: string[];
};

export function getTestEntityRegistry() {
    const entityRegistry = new EntityRegistry();
    entityRegistry.register(new DatasetEntity());
    entityRegistry.register(new UserEntity());
    entityRegistry.register(new TagEntity());
    entityRegistry.register(new DataFlowEntity());
    entityRegistry.register(new DataJobEntity());
    return entityRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);

    return (
        <ThemeProvider theme={defaultThemeConfig}>
            <MemoryRouter initialEntries={initialEntries}>
                <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>
            </MemoryRouter>
        </ThemeProvider>
    );
};
