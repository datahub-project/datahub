import React, { useMemo } from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';

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
    return entityRegistry;
}

export default ({ children, initialEntries }: Props) => {
    const entityRegistry = useMemo(() => getTestEntityRegistry(), []);
    Object.defineProperty(window.document, 'cookie', {
        writable: true,
        value: `${CLIENT_AUTH_COOKIE}=urn:li:corpuser:2`,
    });
    jest.mock('js-cookie', () => ({ get: () => 'urn:li:corpuser:2' }));

    return (
        <ThemeProvider theme={defaultThemeConfig}>
            <MemoryRouter initialEntries={initialEntries}>
                <EntityRegistryContext.Provider value={entityRegistry}>
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
                        }}
                    >
                        {children}
                    </LineageExplorerContext.Provider>
                </EntityRegistryContext.Provider>
            </MemoryRouter>
        </ThemeProvider>
    );
};
