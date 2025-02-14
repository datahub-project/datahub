import VersionsDrawer from '@app/entityV2/shared/versioning/VersionsDrawer';
import LineageGraph from '@app/lineageV2/LineageGraph';
import React, { useCallback, useContext, useState } from 'react';
import { Alert } from 'antd';
import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import useEntityState from '@src/app/entity/shared/useEntityState';
import styled from 'styled-components/macro';
import { useHistory, useLocation } from 'react-router';
import { matchPath } from 'react-router-dom';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { EntityType, Exact } from '../../../../../types.generated';
import { useLineageV2 } from '../../../../lineageV2/useLineageV2';
import {
    getEntityPath,
    getFinalSidebarTabs,
    getOnboardingStepIdsForEntityType,
    useRoutedTab,
    useUpdateGlossaryEntityDataOnChange,
    defaultTabDisplayConfig,
} from './utils';
import { EntityHeader } from './header/EntityHeader';
import { EntityTabs } from './header/EntityTabs';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import CompactContext from '../../../../shared/CompactContext';
import DynamicTab from '../../tabs/Entity/weaklyTypedAspects/DynamicTab';
import analytics, { EventType } from '../../../../analytics';
import { EntityMenuItems } from '../../EntityDropdown/EntityMenuActions';
import { useIsSeparateSiblingsMode } from '../../useIsSeparateSiblingsMode';
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
import EntityProfileSidebar from './sidebar/EntityProfileSidebar';
import { PageRoutes } from '../../../../../conf/Global';
import EntitySidebarContext from '../../../../sharedV2/EntitySidebarContext';
import TabFullsizeContext from '../../../../shared/TabFullsizedContext';
import { useUpdateDomainEntityDataOnChange as useUpdateDomainEntityDataOnChangeV2 } from '../../../../domainV2/utils';
import { EntityContext } from '../../../../entity/shared/EntityContext';
import {
    DrawerType,
    EntitySubHeaderSection,
    GenericEntityProperties,
    GenericEntityUpdate,
} from '../../../../entity/shared/types';
import { EntitySidebarSection, EntitySidebarTab, EntityTab, TabContextType, TabRenderType } from '../../types';

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
    getOverrideProperties?: (T) => GenericEntityProperties;
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
`;

const StyledEntityProfileSidebar = styled(EntityProfileSidebar)<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'padding-bottom: 12px;'}
`;

const HeaderAndTabsFlex = styled.div`
    flex-grow: 1;
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

const Header = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    /* padding: ${(props) => (props.$isShowNavBarRedesign ? '4px 8px 4px 4px' : '0px 16px 0px 16px')}; */
    padding: ${(props) => (props.$isShowNavBarRedesign ? '5px 9px 4px 5px' : '0px 16px 0px 16px')};
    ${(props) => props.$isShowNavBarRedesign && 'margin-right: 0px;'}
    display: flex;
    align-items: center;
`;

const HeaderContent = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 5px rgba(0, 0, 0, 0.08)'};
    flex: 1;
    flex-shrink: 0;
    padding: 0;
`;

const Body = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding: ${(props) => (props.$isShowNavBarRedesign ? '12px 8px 4px 4px' : '12px 16px 12px 16px')};
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const BodyContent = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    flex: 1;
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 5px rgba(0, 0, 0, 0.08)'};
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
    flex-grow: 1;
    min-width: 0;
    height: 100%;
    margin-top: ${({ showAlert }) => (showAlert ? '2.5rem' : '0')};
`;

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
    const history = useHistory();
    const location = useLocation();
    const isInSearch = matchPath(location.pathname, PageRoutes.SEARCH_RESULTS) !== null;
    const [showAlert, setShowAlert] = useState(true);
    const entityState = useEntityState();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [drawer, setDrawer] = useState<DrawerType | undefined>(undefined);

    const { width } = React.useContext(EntitySidebarContext);
    const isCompact = React.useContext(CompactContext);

    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));

    const [shouldRefetchEmbeddedListSearch, setShouldRefetchEmbeddedListSearch] = useState(false);
    const entityStepIds: string[] = getOnboardingStepIdsForEntityType(entityType);
    const lineageGraphStepIds: string[] = [LINEAGE_GRAPH_INTRO_ID, LINEAGE_GRAPH_TIME_FILTER_ID];
    const stepIds = isLineageMode ? lineageGraphStepIds : entityStepIds;

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

    const finalTabs = getFinalSidebarTabs(sidebarTabs, sidebarSections || []);

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
                    entityState,
                }}
            >
                <>
                    {(error && <ErrorSection />) || (
                        <EntityProfileSidebar
                            type={isInSearch ? 'card' : undefined}
                            focused={isInSearch}
                            tabs={finalTabs}
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
                entityState,
                setDrawer,
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
                <OnboardingTour stepIds={stepIds} />
                <EntityHead />
                {showError && <ErrorSection />}
                {showFullScreen && <LineageGraph isFullscreen />}
                {!showFullScreen && (
                    <ContentContainer>
                        {showExplorer && <LineageExplorer type={entityType} urn={urn} />}
                        {!isLineageMode && (
                            <>
                                <HeaderAndTabsFlex>
                                    {!isTabFullsize && (
                                        <Header $isShowNavBarRedesign={isShowNavBarRedesign}>
                                            <HeaderContent $isShowNavBarRedesign={isShowNavBarRedesign}>
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
                                    <Body $isShowNavBarRedesign={isShowNavBarRedesign}>
                                        <BodyContent $isShowNavBarRedesign={isShowNavBarRedesign}>
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
                                {!isTabFullsize && (
                                    <StyledEntityProfileSidebar
                                        tabs={finalTabs}
                                        type="card"
                                        width={width}
                                        contextType={TabContextType.PROFILE_SIDEBAR}
                                        headerDropdownItems={headerDropdownItems}
                                        $isShowNavBarRedesign={isShowNavBarRedesign}
                                    />
                                )}
                            </>
                        )}
                    </ContentContainer>
                )}
            </Wrapper>
            {!!entityData?.versionProperties?.versionSet && (
                <VersionsDrawer
                    versionSetUrn={entityData.versionProperties?.versionSet.urn}
                    open={drawer === DrawerType.VERSIONS}
                />
            )}
        </EntityContext.Provider>
    );
};
