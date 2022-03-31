import React, { useCallback } from 'react';
import { Alert, Divider } from 'antd';
import SplitPane from 'react-split-pane';
import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { EntityType, Exact } from '../../../../../types.generated';
import { Message } from '../../../../shared/Message';
import { getDataForEntityType, getEntityPath, useRoutedTab } from './utils';
import { EntitySidebarSection, EntityTab, GenericEntityProperties, GenericEntityUpdate } from '../../types';
import { EntityProfileNavBar } from './nav/EntityProfileNavBar';
import { ANTD_GRAY } from '../../constants';
import { EntityHeader } from './header/EntityHeader';
import { EntityTabs } from './header/EntityTabs';
import { EntitySidebar } from './sidebar/EntitySidebar';
import EntityContext from '../../EntityContext';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import CompactContext from '../../../../shared/CompactContext';
import DynamicTab from '../../tabs/Entity/weaklyTypedAspects/DynamicTab';
import analytics, { EventType } from '../../../../analytics';

type Props<T, U> = {
    urn: string;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
        }>
    >;
    useUpdateQuery?: (
        baseOptions?: MutationHookOptions<U, { urn: string; input: GenericEntityUpdate }> | undefined,
    ) => MutationTuple<U, { urn: string; input: GenericEntityUpdate }>;
    getOverrideProperties: (T) => GenericEntityProperties;
    tabs: EntityTab[];
    sidebarSections: EntitySidebarSection[];
};

const ContentContainer = styled.div`
    display: flex;
    height: auto;
    min-height: 100%;
    align-items: stretch;
    flex: 1;
`;

const HeaderAndTabs = styled.div`
    flex-basis: 70%;
    min-width: 640px;
    height: 100%;
`;

const HeaderAndTabsFlex = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    height: 100%;
    max-height: 100%;
    overflow: hidden;
    min-height: 0;
`;
const Sidebar = styled.div`
    overflow: auto;
    flex-basis: 30%;
    padding-left: 20px;
    padding-right: 20px;
`;

const Header = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 20px 20px 0 20px;
    flex-shrink: 0;
`;

const TabContent = styled.div`
    flex: 1;
    overflow: auto;
`;

const resizerStyles = {
    borderLeft: `1px solid #E9E9E9`,
    width: '2px',
    cursor: 'col-resize',
    margin: '0 5px',
    height: '100%',
};

const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

const defaultSidebarSection = {
    visible: (_, _1) => true,
};

/**
 * Container for display of the Entity Page
 */
export const EntityProfile = <T, U>({
    urn,
    useEntityQuery,
    useUpdateQuery,
    entityType,
    getOverrideProperties,
    tabs,
    sidebarSections,
}: Props<T, U>): JSX.Element => {
    const isLineageMode = useIsLineageMode();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const isCompact = React.useContext(CompactContext);
    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));
    const sideBarSectionsWithDefaults = sidebarSections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...defaultSidebarSection, ...sidebarSection.display },
    }));

    const routeToTab = useCallback(
        ({
            tabName,
            tabParams,
            method = 'push',
        }: {
            tabName: string;
            tabParams?: Record<string, any>;
            method?: 'push' | 'replace';
        }) => {
            analytics.event({
                type: EventType.EntitySectionViewEvent,
                entityType,
                entityUrn: urn,
                section: tabName.toLowerCase(),
            });
            history[method](getEntityPath(entityType, urn, entityRegistry, false, tabName, tabParams));
        },
        [history, entityType, urn, entityRegistry],
    );

    const { loading, error, data, refetch } = useEntityQuery({
        variables: { urn },
    });

    const maybeUpdateEntity = useUpdateQuery?.({
        onCompleted: () => refetch(),
    });
    let updateEntity;
    if (maybeUpdateEntity) {
        [updateEntity] = maybeUpdateEntity;
    }

    const entityData =
        (data && getDataForEntityType({ data: data[Object.keys(data)[0]], entityType, getOverrideProperties })) || null;

    const lineage = entityData ? entityRegistry.getLineageVizConfig(entityType, entityData) : undefined;

    const autoRenderTabs: EntityTab[] =
        entityData?.autoRenderAspects?.map((aspect) => ({
            name: aspect.renderSpec?.displayName || aspect.aspectName,
            component: () => (
                <DynamicTab
                    renderSpec={aspect.renderSpec}
                    type={aspect.renderSpec?.displayType}
                    payload={aspect.payload}
                />
            ),
            display: {
                visible: () => true,
                enabled: () => true,
            },
        })) || [];

    const routedTab = useRoutedTab([...tabsWithDefaults, ...autoRenderTabs]);

    if (isCompact) {
        return (
            <EntityContext.Provider
                value={{
                    urn,
                    entityType,
                    entityData,
                    baseEntity: data,
                    updateEntity,
                    routeToTab,
                    refetch,
                    lineage,
                }}
            >
                <div>
                    {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
                    {!loading && error && (
                        <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />
                    )}
                    {!loading && (
                        <>
                            <EntityHeader />
                            <Divider />
                            <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                        </>
                    )}
                </div>
            </EntityContext.Provider>
        );
    }

    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);
    const isLineageEnabled = entityRegistry.getLineageEntityTypes().includes(entityType);
    const showBrowseBar = isBrowsable || isLineageEnabled;

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType,
                entityData,
                baseEntity: data,
                updateEntity,
                routeToTab,
                refetch,
                lineage,
            }}
        >
            <>
                {showBrowseBar && <EntityProfileNavBar urn={urn} entityType={entityType} />}
                {entityData?.status?.removed === true && (
                    <Alert
                        message="This entity has been soft deleted and is not discoverable via search or lineage graph"
                        banner
                    />
                )}
                {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
                {!loading && error && (
                    <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />
                )}
                <ContentContainer>
                    {isLineageMode ? (
                        <LineageExplorer type={entityType} urn={urn} />
                    ) : (
                        <SplitPane
                            split="vertical"
                            minSize={window.innerWidth - 400}
                            maxSize={window.innerWidth - 250}
                            defaultSize={window.innerWidth - 400}
                            resizerStyle={resizerStyles}
                            style={{
                                position: 'inherit',
                                height: 'auto',
                                overflow: 'auto',
                            }}
                        >
                            <HeaderAndTabs>
                                <HeaderAndTabsFlex>
                                    <Header>
                                        <EntityHeader />
                                        <EntityTabs
                                            tabs={[...tabsWithDefaults, ...autoRenderTabs]}
                                            selectedTab={routedTab}
                                        />
                                    </Header>
                                    <TabContent>
                                        {routedTab && <routedTab.component properties={routedTab.properties} />}
                                    </TabContent>
                                </HeaderAndTabsFlex>
                            </HeaderAndTabs>
                            <Sidebar>
                                <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                            </Sidebar>
                        </SplitPane>
                    )}
                </ContentContainer>
            </>
        </EntityContext.Provider>
    );
};
