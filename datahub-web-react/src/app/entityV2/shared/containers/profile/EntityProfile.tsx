import React, { useCallback, useContext, useState } from 'react';
import { Alert } from 'antd';
import { ReadOutlined } from '@ant-design/icons';
import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import styled from 'styled-components/macro';
import { useHistory, useLocation } from 'react-router';
import { matchPath } from 'react-router-dom';

import { EntityType, Exact } from '../../../../../types.generated';
import LineageFullscreen from '../../../../lineageV2/LineageFullscreen';
import { useLineageV2 } from '../../../../lineageV2/useLineageV2';
import {
    getEntityPath,
    getOnboardingStepIdsForEntityType,
    useRoutedTab,
    useUpdateGlossaryEntityDataOnChange,
} from './utils';
import {
    EntitySidebarSection,
    EntitySubHeaderSection,
    EntitySidebarTab,
    EntityTab,
    GenericEntityProperties,
    GenericEntityUpdate,
    TabContextType,
    TabRenderType,
} from '../../types';
import { EntityHeader } from './header/EntityHeader';
import { EntityTabs } from './header/EntityTabs';
import { EntityContext } from '../../EntityContext';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import CompactContext from '../../../../shared/CompactContext';
import DynamicTab from '../../tabs/Entity/weaklyTypedAspects/DynamicTab';
import analytics, { EventType } from '../../../../analytics';
import { EntityMenuItems } from '../../EntityDropdown/EntityMenuActions';
import { useIsSeparateSiblingsMode } from '../../siblingUtils';
import { EntityActionItem } from '../../entity/EntityActions';
import { ErrorSection } from '../../../../shared/error/ErrorSection';
import { EntityHead } from '../../../../shared/EntityHead';
import { OnboardingTour } from '../../../../onboarding/OnboardingTour';
import useGetDataForProfile from './useGetDataForProfile';
import NonExistentEntityPage from '../../entity/NonExistentEntityPage';
import {
    LINEAGE_GRAPH_INTRO_ID,
    LINEAGE_GRAPH_TIME_FILTER_ID,
} from '../../../../onboarding/config/LineageGraphOnboardingConfig';
import { useSubscriptionsEnabled } from '../../../../settings/personal/notifications/utils';
import { ENTITY_PROFILE_SUBSCRIPTION_ID } from '../../../../onboarding/config/EntityProfileOnboardingConfig';
import EntityProfileSidebar from './sidebar/EntityProfileSidebar';
import EntitySidebarSectionsTab from './sidebar/EntitySidebarSectionsTab';
import { PageRoutes } from '../../../../../conf/Global';
import EntitySidebarContext from '../../../../shared/EntitySidebarContext';
import TabFullsizeContext from '../../../../shared/TabFullsizedContext';
import { useUpdateDomainEntityDataOnChange as useUpdateDomainEntityDataOnChangeV2 } from '../../../../domainV2/utils';

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
        baseOptions?:
            | MutationHookOptions<
                  U,
                  {
                      urn: string;
                      input: GenericEntityUpdate;
                  }
              >
            | undefined,
    ) => MutationTuple<U, { urn: string; input: GenericEntityUpdate }>;
    getOverrideProperties: (T) => GenericEntityProperties;
    tabs: EntityTab[];
    sidebarTabs?: EntitySidebarTab[];
    sidebarSections?: EntitySidebarSection[]; // Deprecated.
    subHeader?: EntitySubHeaderSection;
    headerDropdownItems?: Set<EntityMenuItems>;
    headerActionItems?: Set<EntityActionItem>;
    isNameEditable?: boolean;
    isIconEditable?: boolean;
    isColorEditable?: boolean;
};

const ContentContainer = styled.div`
    display: flex;
    height: 100%;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const SidebarWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: calc(100vh - 72px);
`;

const HeaderAndTabs = styled.div`
    flex-grow: 1;
    min-width: 640px;
`;

const HeaderAndTabsFlex = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    max-height: 100%;
    overflow: hidden;
    min-height: 0;
    overflow-y: auto;
    /* Hide scrollbar for Chrome, Safari, and Opera */

    &::-webkit-scrollbar {
        display: none;
    }
`;

const Header = styled.div`
    padding: 0px 16px 0px 16px;
    display: flex;
    align-items: center;
`;

const HeaderContent = styled.div`
    background-color: #ffffff;
    border-radius: 8px;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    flex: 1;
    flex-shrink: 0;
    padding: 0;
`;

const Body = styled.div`
    padding: 12px 16px 12px 16px;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const BodyContent = styled.div`
    background-color: #ffffff;
    border-radius: 8px;
    display: flex;
    flex-direction: column;
    flex: 1;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    height: 100%;
    overflow: hidden;
`;

const TabsWrapper = styled.div``;

const TabContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
`;

const StyledAlert = styled(Alert)`
    box-sizing: border-box;
    position: fixed;
    width: calc(100% - 70px);
    z-index: 1000;
`;

const Wrapper = styled.div<{ showAlert: boolean }>`
    height: 100%;
    width: 100%;
    margin-top: ${({ showAlert }) => (showAlert ? '2.5rem' : '0')};
`;

export const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

export const DEFAULT_SIDEBAR_SECTION = {
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
    sidebarTabs = [],
    sidebarSections,
    headerDropdownItems,
    headerActionItems,
    isNameEditable,
    isColorEditable,
    isIconEditable,
    subHeader,
}: Props<T, U>): JSX.Element => {
    const { isTabFullsize } = useContext(TabFullsizeContext);
    const isLineageMode = useIsLineageMode();
    const isLineageV2 = useLineageV2();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const entityRegistry = useEntityRegistry();
    const subscriptionsEnabled = useSubscriptionsEnabled();
    const history = useHistory();
    const location = useLocation();
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const [showAlert, setShowAlert] = useState(true);

    const { width } = React.useContext(EntitySidebarContext);
    const isCompact = React.useContext(CompactContext);

    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));
    const sidebarTabsWithDefaults = sidebarTabs.map((tab) => ({
        ...tab,
        display: { ...defaultTabDisplayConfig, ...tab.display },
    }));

    const [shouldRefetchEmbeddedListSearch, setShouldRefetchEmbeddedListSearch] = useState(false);
    const entityStepIds: string[] = getOnboardingStepIdsForEntityType(entityType);
    const lineageGraphStepIds: string[] = [LINEAGE_GRAPH_INTRO_ID, LINEAGE_GRAPH_TIME_FILTER_ID];
    const stepIds = isLineageMode ? lineageGraphStepIds : entityStepIds;
    const filteredStepIds = subscriptionsEnabled
        ? stepIds
        : stepIds.filter((id) => id !== ENTITY_PROFILE_SUBSCRIPTION_ID);

    const routeToTab = useCallback(
        ({
            tabName,
            tabParams,
            method = 'replace',
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
            history[method](
                getEntityPath(entityType, urn, entityRegistry, false, isHideSiblingMode, tabName, tabParams),
            );
        },
        [history, entityType, urn, entityRegistry, isHideSiblingMode],
    );

    const { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, error, refetch } =
        useGetDataForProfile({ urn, entityType, useEntityQuery, getOverrideProperties });

    useUpdateGlossaryEntityDataOnChange(entityData, entityType);
    useUpdateDomainEntityDataOnChangeV2(entityData, entityType);

    const maybeUpdateEntity = useUpdateQuery?.({
        onCompleted: () => refetch(),
    });

    let updateEntity;
    if (maybeUpdateEntity) {
        [updateEntity] = maybeUpdateEntity;
    }

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

    const visibleTabs = [...tabsWithDefaults, ...autoRenderTabs].filter((tab) =>
        tab.display?.visible(entityData, dataPossiblyCombinedWithSiblings),
    );

    const enabledAndVisibleTabs = visibleTabs.filter((tab) =>
        tab.display?.enabled(entityData, dataPossiblyCombinedWithSiblings),
    );

    const routedTab = useRoutedTab(enabledAndVisibleTabs);

    if (entityData?.exists === false) {
        return <NonExistentEntityPage />;
    }

    let finalTabs = sidebarTabs;

    // Add a default "About" tab if only the legacy sections were provided.
    if ((sidebarSections || [])?.length > 0) {
        finalTabs = [
            {
                name: 'About',
                icon: ReadOutlined,
                component: EntitySidebarSectionsTab,
                properties: {
                    sections: sidebarSections || [],
                },
                display: {
                    ...defaultTabDisplayConfig,
                },
            },
            ...sidebarTabsWithDefaults,
        ];
    }

    if (isCompact) {
        return (
            <EntityContext.Provider
                value={{
                    urn,
                    entityType,
                    entityData,
                    loading,
                    baseEntity: dataPossiblyCombinedWithSiblings,
                    dataNotCombinedWithSiblings,
                    updateEntity,
                    routeToTab,
                    refetch,
                    lineage,
                    shouldRefetchEmbeddedListSearch,
                    setShouldRefetchEmbeddedListSearch,
                }}
            >
                <>
                    {(error && <ErrorSection />) || (
                        <EntityProfileSidebar
                            type={isInSearch ? 'card' : undefined}
                            focused={isInSearch}
                            tabs={finalTabs}
                            // TODO: Hide collapse for chrome extension
                            contextType={isInSearch ? TabContextType.SEARCH_SIDEBAR : TabContextType.LINEAGE_SIDEBAR}
                            width={width}
                            headerDropdownItems={headerDropdownItems}
                        />
                    )}
                </>
            </EntityContext.Provider>
        );
    }

    const showError = error;
    const showFullScreen = !error && isLineageMode && isLineageV2;
    const showExplorer = isLineageMode && !isLineageV2;

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType,
                entityData,
                loading,
                baseEntity: dataPossiblyCombinedWithSiblings,
                dataNotCombinedWithSiblings,
                updateEntity,
                routeToTab,
                refetch,
                lineage,
                shouldRefetchEmbeddedListSearch,
                setShouldRefetchEmbeddedListSearch,
            }}
        >
            {entityData?.status?.removed && (
                <StyledAlert
                    message="This entity is not discoverable via search or lineage graph. Contact your DataHub admin for more information."
                    banner
                    closable
                    onClose={() => setShowAlert(false)}
                />
            )}
            <Wrapper showAlert={showAlert && !!entityData?.status?.removed}>
                <OnboardingTour stepIds={filteredStepIds} />
                <EntityHead />
                {showError && <ErrorSection />}
                {showFullScreen && <LineageFullscreen urn={urn} type={entityType} />}
                {!showFullScreen && (
                    <ContentContainer>
                        {showExplorer && <LineageExplorer type={entityType} urn={urn} />}
                        {!isLineageMode && (
                            <>
                                <HeaderAndTabs>
                                    <HeaderAndTabsFlex>
                                        {!isTabFullsize && (
                                            <Header>
                                                <HeaderContent>
                                                    <EntityHeader
                                                        headerDropdownItems={headerDropdownItems}
                                                        headerActionItems={headerActionItems}
                                                        isNameEditable={isNameEditable}
                                                        isIconEditable={isIconEditable}
                                                        isColorEditable={isColorEditable}
                                                        displayProperties={entityData?.displayProperties || undefined}
                                                        subHeader={subHeader}
                                                    />
                                                </HeaderContent>
                                            </Header>
                                        )}
                                        <Body>
                                            <BodyContent>
                                                {!isTabFullsize && (
                                                    <TabsWrapper>
                                                        <EntityTabs tabs={visibleTabs} selectedTab={routedTab} />
                                                    </TabsWrapper>
                                                )}
                                                <TabContent>
                                                    {routedTab && (
                                                        <routedTab.component
                                                            properties={routedTab.properties}
                                                            contextType={TabContextType.PROFILE}
                                                            renderType={TabRenderType.DEFAULT}
                                                        />
                                                    )}
                                                </TabContent>
                                            </BodyContent>
                                        </Body>
                                    </HeaderAndTabsFlex>
                                </HeaderAndTabs>
                                {!isTabFullsize && (
                                    <SidebarWrapper>
                                        <EntityProfileSidebar
                                            tabs={finalTabs}
                                            type="card"
                                            width={
                                                width ||
                                                (finalTabs.length > 1
                                                    ? window.innerWidth * 0.33
                                                    : window.innerWidth * 0.25)
                                            }
                                            contextType={TabContextType.PROFILE_SIDEBAR}
                                            headerDropdownItems={headerDropdownItems}
                                        />
                                    </SidebarWrapper>
                                )}
                            </>
                        )}
                    </ContentContainer>
                )}
            </Wrapper>
        </EntityContext.Provider>
    );
};
