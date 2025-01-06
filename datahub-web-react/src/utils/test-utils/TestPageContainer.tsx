import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';

import { Remirror, useRemirror } from '@remirror/react';
import { ItalicExtension, UnderlineExtension } from 'remirror/extensions';

import { HelmetProvider } from 'react-helmet-async';
import EntityRegistry from '../../app/entity/EntityRegistry';
import { DataFlowEntity } from '../../app/entity/dataFlow/DataFlowEntity';
import { DataJobEntity } from '../../app/entity/dataJob/DataJobEntity';
import { DatasetEntity } from '../../app/entity/dataset/DatasetEntity';
import { GroupEntity } from '../../app/entity/group/Group';
import { TagEntity } from '../../app/entity/tag/Tag';
import { UserEntity } from '../../app/entity/user/User';
import { CLIENT_AUTH_COOKIE } from '../../conf/Global';
import { EntityRegistryContext } from '../../entityRegistryContext';

import AppConfigProvider from '../../AppConfigProvider';
import CustomThemeProvider from '../../CustomThemeProvider';
import UserContextProvider from '../../app/context/UserContextProvider';
import { BusinessAttributeEntity } from '../../app/entity/businessAttribute/BusinessAttributeEntity';
import { ChartEntity } from '../../app/entity/chart/ChartEntity';
import { ContainerEntity } from '../../app/entity/container/ContainerEntity';
import { DashboardEntity } from '../../app/entity/dashboard/DashboardEntity';
import { DataPlatformEntity } from '../../app/entity/dataPlatform/DataPlatformEntity';
import { DataPlatformInstanceEntity } from '../../app/entity/dataPlatformInstance/DataPlatformInstanceEntity';
import { DataProductEntity } from '../../app/entity/dataProduct/DataProductEntity';
import { DomainEntity } from '../../app/entity/domain/DomainEntity';
import { GlossaryTermEntity } from '../../app/entity/glossaryTerm/GlossaryTermEntity';
import { MLFeatureTableEntity } from '../../app/entity/mlFeatureTable/MLFeatureTableEntity';
import { MLModelEntity } from '../../app/entity/mlModel/MLModelEntity';
import { MLModelGroupEntity } from '../../app/entity/mlModelGroup/MLModelGroupEntity';
import { QueryEntity } from '../../app/entity/query/QueryEntity';
import { SchemaFieldPropertiesEntity } from '../../app/entity/schemaField/SchemaFieldPropertiesEntity';
import { StructuredPropertyEntity } from '../../app/entity/structuredProperty/StructuredPropertyEntity';
import { LineageExplorerContext } from '../../app/lineage/utils/LineageExplorerContext';

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
    entityRegistry.register(new DomainEntity());
    entityRegistry.register(new DataProductEntity());
    entityRegistry.register(new DataPlatformInstanceEntity());
    entityRegistry.register(new QueryEntity());
    entityRegistry.register(new SchemaFieldPropertiesEntity());
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
