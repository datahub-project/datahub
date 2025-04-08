import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import { Alert, Divider } from 'antd';
import React, { useCallback, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useUpdateDomainEntityDataOnChange } from '@app/domain/utils';
import { EntityContext } from '@app/entity/shared/EntityContext';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { EntityHeader } from '@app/entity/shared/containers/profile/header/EntityHeader';
import { EntityTabs } from '@app/entity/shared/containers/profile/header/EntityTabs';
import { EntityProfileNavBar } from '@app/entity/shared/containers/profile/nav/EntityProfileNavBar';
import { EntitySidebar } from '@app/entity/shared/containers/profile/sidebar/EntitySidebar';
import SidebarFormInfoWrapper from '@app/entity/shared/containers/profile/sidebar/FormInfo/SidebarFormInfoWrapper';
import ProfileSidebar from '@app/entity/shared/containers/profile/sidebar/ProfileSidebar';
import useGetDataForProfile from '@app/entity/shared/containers/profile/useGetDataForProfile';
import {
    getEntityPath,
    getOnboardingStepIdsForEntityType,
    sortEntityProfileTabs,
    useRoutedTab,
    useUpdateGlossaryEntityDataOnChange,
} from '@app/entity/shared/containers/profile/utils';
import { EntityActionItem } from '@app/entity/shared/entity/EntityActions';
import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import DynamicTab from '@app/entity/shared/tabs/Entity/weaklyTypedAspects/DynamicTab';
import {
    EntitySidebarSection,
    EntitySubHeaderSection,
    EntityTab,
    GenericEntityProperties,
    GenericEntityUpdate,
} from '@app/entity/shared/types';
import LineageExplorer from '@app/lineage/LineageExplorer';
import useIsLineageMode from '@app/lineage/utils/useIsLineageMode';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    LINEAGE_GRAPH_INTRO_ID,
    LINEAGE_GRAPH_TIME_FILTER_ID,
} from '@app/onboarding/config/LineageGraphOnboardingConfig';
import CompactContext from '@app/shared/CompactContext';
import { EntityHead } from '@app/shared/EntityHead';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Exact } from '@types';

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

const MAX_COMPACT_WIDTH = 490 - 24 * 2;

const ContentContainer = styled.div`
    display: flex;
    height: auto;
    min-height: 100%;
    flex: 1;
    min-width: 0;
    overflow: hidden;
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
    sidebarSections,
    headerDropdownItems,
    headerActionItems,
    isNameEditable,
    hideBrowseBar,
    subHeader,
}: Props<T, U>): JSX.Element => {
    const { config } = useAppConfig();
    const { erModelRelationshipFeatureEnabled } = config.featureFlags;
    const isLineageMode = useIsLineageMode();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const appConfig = useAppConfig();
    const isCompact = React.useContext(CompactContext);
    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));

    if (erModelRelationshipFeatureEnabled) {
        const relationIndex = tabsWithDefaults.findIndex((tab) => {
            return tab.name === 'Relationships';
        });
        if (relationIndex >= 0) {
            tabsWithDefaults[relationIndex] = {
                ...tabsWithDefaults[relationIndex],
                display: { ...defaultTabDisplayConfig },
            };
        }
    }

    const sortedTabs = sortEntityProfileTabs(appConfig.config, entityType, tabsWithDefaults);
    const sideBarSectionsWithDefaults = sidebarSections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...DEFAULT_SIDEBAR_SECTION, ...sidebarSection.display },
    }));

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
                        message={
                            <>
                                This entity is marked as soft-deleted, likely due to stateful ingestion or a manual
                                deletion command, and will not appear in search or lineage graphs. Contact your DataHub
                                admin for more information.
                            </>
                        }
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
                                <ProfileSidebar
                                    sidebarSections={sidebarSections}
                                    topSection={{ component: SidebarFormInfoWrapper }}
                                />
                            </>
                        )}
                    </ContentContainer>
                )}
            </>
        </EntityContext.Provider>
    );
};
