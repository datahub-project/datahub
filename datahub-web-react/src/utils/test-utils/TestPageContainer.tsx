import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';

import { HelmetProvider } from 'react-helmet-async';
import { CLIENT_AUTH_COOKIE } from '../../conf/Global';
import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { DataFlowEntity } from '../../app/entity/dataFlow/DataFlowEntity';
import { DataJobEntity } from '../../app/entity/dataJob/DataJobEntity';
import { UserEntity } from '../../app/entity/user/User';
import { GroupEntity } from '../../app/entity/group/Group';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { EntityRegistryContext } from '../../entityRegistryContext';
import { TagEntity } from '../../app/entity/tag/Tag';

import defaultThemeConfig from '../../conf/theme/theme_light.config.json';
import { GlossaryTermEntity } from '../../app/entity/glossaryTerm/GlossaryTermEntity';
import { MLFeatureTableEntity } from '../../app/entity/mlFeatureTable/MLFeatureTableEntity';
import { MLModelEntity } from '../../app/entity/mlModel/MLModelEntity';
import { MLModelGroupEntity } from '../../app/entity/mlModelGroup/MLModelGroupEntity';
import { ChartEntity } from '../../app/entity/chart/ChartEntity';
import { DashboardEntity } from '../../app/entity/dashboard/DashboardEntity';
import { LineageExplorerContext } from '../../app/lineage/utils/LineageExplorerContext';
import UserContextProvider from '../../app/context/UserContextProvider';
import { DataPlatformEntity } from '../../app/entity/dataPlatform/DataPlatformEntity';
import { ContainerEntity } from '../../app/entity/container/ContainerEntity';
import AppConfigProvider from '../../AppConfigProvider';
import { BusinessAttributeEntity } from '../../app/entity/businessAttribute/BusinessAttributeEntity';
import { SchemaFieldPropertiesEntity } from '../../app/entity/schemaField/SchemaFieldPropertiesEntity';

type Props = {
    children: React.ReactNode;
    initialEntries?: string[];
};

export function getTestEntityRegistry() {
    const entityRegistry = new EntityRegistry();
    entityRegistry.register(new DatasetEntity());
    entityRegistry.register(new ChartEntity());
    entityRegistry.register(new DashboardEntity());
    entityRegistry.register(new UserEntity());
    entityRegistry.register(new GroupEntity());
    entityRegistry.register(new TagEntity());
    entityRegistry.register(new DataFlowEntity());
    entityRegistry.register(new DataJobEntity());
    entityRegistry.register(new GlossaryTermEntity());
    entityRegistry.register(new MLFeatureTableEntity());
    entityRegistry.register(new MLModelEntity());
    entityRegistry.register(new MLModelGroupEntity());
    entityRegistry.register(new DataPlatformEntity());
    entityRegistry.register(new ContainerEntity());
    entityRegistry.register(new BusinessAttributeEntity());
    entityRegistry.register(new SchemaFieldPropertiesEntity());
    return entityRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);
    Object.defineProperty(window.document, 'cookie', {
        writable: true,
        value: `${CLIENT_AUTH_COOKIE}=urn:li:corpuser:2`,
    });
    vi.mock('js-cookie', () => ({ default: { get: () => 'urn:li:corpuser:2' } }));

    return (
        <HelmetProvider>
            <ThemeProvider theme={defaultThemeConfig}>
                <MemoryRouter initialEntries={initialEntries}>
                    <EntityRegistryContext.Provider value={entityRegistry}>
                        <UserContextProvider>
                            <AppConfigProvider>
                                <LineageExplorerContext.Provider
                                    value={{
                                        expandTitles: false,
                                        showColumns: false,
                                        collapsedColumnsNodes: {},
                                        setCollapsedColumnsNodes: null,
                                        fineGrainedMap: {},
                                        selectedField: null,
                                        setSelectedField: () => {},
                                        highlightedEdges: [],
                                        setHighlightedEdges: () => {},
                                        visibleColumnsByUrn: {},
                                        setVisibleColumnsByUrn: () => {},
                                        columnsByUrn: {},
                                        setColumnsByUrn: () => {},
                                        refetchCenterNode: () => {},
                                    }}
                                >
                                    {children}
                                </LineageExplorerContext.Provider>
                            </AppConfigProvider>
                        </UserContextProvider>
                    </EntityRegistryContext.Provider>
                </MemoryRouter>
            </ThemeProvider>
        </HelmetProvider>
    );
};
