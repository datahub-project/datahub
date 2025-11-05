import { Remirror, useRemirror } from '@remirror/react';
import React, { useMemo } from 'react';
import { HelmetProvider } from 'react-helmet-async';
import { MemoryRouter } from 'react-router';
import { ItalicExtension, UnderlineExtension } from 'remirror/extensions';

import UserContextProvider from '@app/context/UserContextProvider';
import EntityRegistry from '@app/entityV2/EntityRegistry';
import { BusinessAttributeEntity } from '@app/entityV2/businessAttribute/BusinessAttributeEntity';
import { ChartEntity } from '@app/entityV2/chart/ChartEntity';
import { ContainerEntity } from '@app/entityV2/container/ContainerEntity';
import { DashboardEntity } from '@app/entityV2/dashboard/DashboardEntity';
import { DataFlowEntity } from '@app/entityV2/dataFlow/DataFlowEntity';
import { DataJobEntity } from '@app/entityV2/dataJob/DataJobEntity';
import { DataPlatformEntity } from '@app/entityV2/dataPlatform/DataPlatformEntity';
import { DataPlatformInstanceEntity } from '@app/entityV2/dataPlatformInstance/DataPlatformInstanceEntity';
import { DataProductEntity } from '@app/entityV2/dataProduct/DataProductEntity';
import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';
import { DomainEntity } from '@app/entityV2/domain/DomainEntity';
import GlossaryNodeEntity from '@app/entityV2/glossaryNode/GlossaryNodeEntity';
import { GlossaryTermEntity } from '@app/entityV2/glossaryTerm/GlossaryTermEntity';
import { GroupEntity } from '@app/entityV2/group/Group';
import { MLFeatureTableEntity } from '@app/entityV2/mlFeatureTable/MLFeatureTableEntity';
import { MLModelEntity } from '@app/entityV2/mlModel/MLModelEntity';
import { MLModelGroupEntity } from '@app/entityV2/mlModelGroup/MLModelGroupEntity';
import { QueryEntity } from '@app/entityV2/query/QueryEntity';
import { SchemaFieldEntity } from '@app/entityV2/schemaField/SchemaFieldEntity';
import { StructuredPropertyEntity } from '@app/entityV2/structuredProperty/StructuredPropertyEntity';
import { TagEntity } from '@app/entityV2/tag/Tag';
import { UserEntity } from '@app/entityV2/user/User';
import { LineageExplorerContext } from '@app/lineage/utils/LineageExplorerContext';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';
import AppConfigProvider from '@src/AppConfigProvider';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';

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
    entityRegistry.register(new GlossaryNodeEntity());
    entityRegistry.register(new GlossaryTermEntity());
    entityRegistry.register(new MLFeatureTableEntity());
    entityRegistry.register(new MLModelEntity());
    entityRegistry.register(new MLModelGroupEntity());
    entityRegistry.register(new DataPlatformEntity());
    entityRegistry.register(new ContainerEntity());
    entityRegistry.register(new BusinessAttributeEntity());
    entityRegistry.register(new SchemaFieldEntity());
    entityRegistry.register(new DomainEntity());
    entityRegistry.register(new DataProductEntity());
    entityRegistry.register(new DataPlatformInstanceEntity());
    entityRegistry.register(new QueryEntity());
    entityRegistry.register(new StructuredPropertyEntity());
    return entityRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);
    Object.defineProperty(window.document, 'cookie', {
        writable: true,
        value: `${CLIENT_AUTH_COOKIE}=urn:li:corpuser:2`,
    });
    vi.mock('js-cookie', () => ({ default: { get: () => 'urn:li:corpuser:2' } }));

    // mock remirror
    const extensions = () => [new ItalicExtension(), new UnderlineExtension()];
    const { manager, state } = useRemirror({
        extensions,
    });

    return (
        <HelmetProvider>
            <CustomThemeProvider>
                <MemoryRouter initialEntries={initialEntries}>
                    <EntityRegistryContext.Provider value={entityRegistry}>
                        <UserContextProvider>
                            <AppConfigProvider>
                                <Remirror manager={manager} state={state}>
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
                                </Remirror>
                            </AppConfigProvider>
                        </UserContextProvider>
                    </EntityRegistryContext.Provider>
                </MemoryRouter>
            </CustomThemeProvider>
        </HelmetProvider>
    );
};
