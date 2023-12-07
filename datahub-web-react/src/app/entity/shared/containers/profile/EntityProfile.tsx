import React, { useCallback, useState } from 'react';
import { Alert, Divider } from 'antd';
import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { EntityType, Exact } from '../../../../../types.generated';
import {
    getEntityPath,
    getOnboardingStepIdsForEntityType,
    sortEntityProfileTabs,
    useRoutedTab,
    useUpdateGlossaryEntityDataOnChange,
} from './utils';
import {
    EntitySidebarSection,
    EntitySubHeaderSection,
    EntityTab,
    GenericEntityProperties,
    GenericEntityUpdate,
} from '../../types';
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
import { ProfileSidebarResizer } from './sidebar/ProfileSidebarResizer';
import { EntityMenuItems } from '../../EntityDropdown/EntityDropdown';
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
import { useAppConfig } from '../../../../useAppConfig';
import { useUpdateDomainEntityDataOnChange } from '../../../../domain/utils';

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
    subHeader?: EntitySubHeaderSection;
    headerDropdownItems?: Set<EntityMenuItems>;
    headerActionItems?: Set<EntityActionItem>;
    hideBrowseBar?: boolean;
    isNameEditable?: boolean;
};

const MAX_SIDEBAR_WIDTH = 800;
const MIN_SIDEBAR_WIDTH = 200;
const MAX_COMPACT_WIDTH = 490 - 24 * 2;

const ContentContainer = styled.div`
    display: flex;
    height: auto;
    min-height: 100%;
    flex: 1;
    min-width: 0;
`;

const HeaderAndTabs = styled.div`
    flex-grow: 1;
    min-width: 640px;
`;

const HeaderAndTabsFlex = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    height: 100%;
    max-height: 100%;
    overflow: hidden;
    min-height: 0;
    overflow-y: auto;

    &::-webkit-scrollbar {
        height: 12px;
        width: 2px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;
const Sidebar = styled.div<{ $width: number }>`
    max-height: 100%;
    overflow: auto;
    width: ${(props) => props.$width}px;
    min-width: ${(props) => props.$width}px;
    padding-left: 20px;
    padding-right: 20px;
    padding-bottom: 20px;
`;

const Header = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 20px 20px 0 20px;
    flex-shrink: 0;
`;

const TabContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
`;

const CompactProfile = styled.div`
    max-width: ${MAX_COMPACT_WIDTH};
`;

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
    headerDropdownItems,
    headerActionItems,
    isNameEditable,
    hideBrowseBar,
    subHeader,
}: Props<T, U>): JSX.Element => {
    const isLineageMode = useIsLineageMode();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const appConfig = useAppConfig();
    const isCompact = React.useContext(CompactContext);
    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));
    const sortedTabs = sortEntityProfileTabs(appConfig.config, entityType, tabsWithDefaults);
    const sideBarSectionsWithDefaults = sidebarSections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...defaultSidebarSection, ...sidebarSection.display },
    }));

    const [shouldRefetchEmbeddedListSearch, setShouldRefetchEmbeddedListSearch] = useState(false);
    const [sidebarWidth, setSidebarWidth] = useState(window.innerWidth * 0.25);
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
    useUpdateDomainEntityDataOnChange(entityData, entityType);

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
            getDynamicName: () => '',
        })) || [];

    const visibleTabs = [...sortedTabs, ...autoRenderTabs].filter((tab) =>
        tab.display?.visible(entityData, dataPossiblyCombinedWithSiblings),
    );

    const enabledAndVisibleTabs = visibleTabs.filter((tab) =>
        tab.display?.enabled(entityData, dataPossiblyCombinedWithSiblings),
    );

    const routedTab = useRoutedTab(enabledAndVisibleTabs);

    if (entityData?.exists === false) {
        return <NonExistentEntityPage />;
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
                    {(error && <ErrorSection />) ||
                        (!loading && (
                            <CompactProfile>
                                <EntityHeader
                                    headerDropdownItems={headerDropdownItems}
                                    headerActionItems={headerActionItems}
                                    subHeader={subHeader}
                                />
                                <Divider style={{ marginBottom: '0' }} />
                                <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                            </CompactProfile>
                        ))}
                </>
            </EntityContext.Provider>
        );
    }

    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);
    const isLineageEnabled = entityRegistry.getLineageEntityTypes().includes(entityType);
    const showBrowseBar = (isBrowsable || isLineageEnabled) && !hideBrowseBar;

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
                <OnboardingTour stepIds={stepIds} />
                <EntityHead />
                {showBrowseBar && <EntityProfileNavBar urn={urn} entityType={entityType} />}
                {entityData?.status?.removed === true && (
                    <Alert
                        message="This entity is not discoverable via search or lineage graph. Contact your DataHub admin for more information."
                        banner
                    />
                )}
                {(error && <ErrorSection />) || (
                    <ContentContainer>
                        {isLineageMode ? (
                            <LineageExplorer type={entityType} urn={urn} />
                        ) : (
                            <>
                                <HeaderAndTabs>
                                    <HeaderAndTabsFlex>
                                        <Header>
                                            <EntityHeader
                                                headerDropdownItems={headerDropdownItems}
                                                headerActionItems={headerActionItems}
                                                isNameEditable={isNameEditable}
                                                subHeader={subHeader}
                                            />
                                            <EntityTabs tabs={visibleTabs} selectedTab={routedTab} />
                                        </Header>
                                        <TabContent>
                                            {routedTab && <routedTab.component properties={routedTab.properties} />}
                                        </TabContent>
                                    </HeaderAndTabsFlex>
                                </HeaderAndTabs>
                                <ProfileSidebarResizer
                                    setSidePanelWidth={(width) =>
                                        setSidebarWidth(Math.min(Math.max(width, MIN_SIDEBAR_WIDTH), MAX_SIDEBAR_WIDTH))
                                    }
                                    initialSize={sidebarWidth}
                                />
                                <Sidebar $width={sidebarWidth}>
                                    <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                                </Sidebar>
                            </>
                        )}
                    </ContentContainer>
                )}
            </>
        </EntityContext.Provider>
    );
};
