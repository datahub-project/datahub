import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';

import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { UserEntity } from '../../app/entity/user/User';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { EntityRegistryContext } from '../../entityRegistryContext';
import { TagEntity } from '../../app/entity/tag/Tag';

import defaultThemeConfig from '../../conf/theme/theme_light.config.json';
import { Theme } from '../../conf/theme/types';

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

    const theme: Theme = useMemo(() => {
        const overridesWithoutPrefix: { [key: string]: any } = {};
        const themeConfig = defaultThemeConfig;
        Object.assign(overridesWithoutPrefix, themeConfig.styles);
        Object.keys(overridesWithoutPrefix).forEach((key) => {
            overridesWithoutPrefix[key.substring(1)] = overridesWithoutPrefix[key];
            delete overridesWithoutPrefix[key];
        });
        return {
            ...themeConfig,
            styles: overridesWithoutPrefix as Theme['styles'],
        };
    }, []);

    return (
        <ThemeProvider theme={theme}>
            <MemoryRouter initialEntries={initialEntries}>
                <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>
            </MemoryRouter>
        </ThemeProvider>
    );
};
