import { Button, PageTitle, Tabs, Tooltip } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ExecutionsTab } from '@app/ingestV2/executions/ExecutionsTab';
import { useIngestionOnboardingRedesignV1 } from '@app/ingestV2/hooks/useIngestionOnboardingRedesignV1';
import { SecretsList } from '@app/ingestV2/secret/SecretsList';
import { IngestionSourceList } from '@app/ingestV2/source/IngestionSourceList';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    INGESTION_CREATE_SOURCE_ID,
    INGESTION_REFRESH_SOURCES_ID,
} from '@app/onboarding/config/IngestionOnboardingConfig';
import { NoPageFound } from '@app/shared/NoPageFound';
import { useUrlQueryParam } from '@app/shared/useUrlQueryParam';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow: hidden;
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        height: 100%;
    `}
`;

const PageContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    flex: 1;
    margin: 0 20px 20px 20px;
    height: calc(100% - 80px);
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 20px;
        padding-right: 20px;
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 16px;
    }
`;

const TitleContainer = styled.div`
    flex: 1;
`;

const HeaderActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export const ManageIngestionPage = () => {
    /**
     * Determines which view should be visible: ingestion sources or secrets.
     */
    const { platformPrivileges, loaded: loadedPlatformPrivileges } = useUserContext();
    const { config, loaded: loadedAppConfig } = useAppConfig();
    const location = useLocation();
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const canManageIngestion = platformPrivileges?.manageIngestion;
    const showIngestionTab = isIngestionEnabled && canManageIngestion;
    const showSecretsTab = isIngestionEnabled && platformPrivileges?.manageSecrets;
    const showIngestionOnboardingRedesignV1 = useIngestionOnboardingRedesignV1();

    // undefined == not loaded, null == no permissions
    const [selectedTab, setSelectedTab] = useState<TabType | undefined | null>();

    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const history = useHistory();
    const shouldPreserveParams = useRef(false);

    // Use URL query param hooks for state management
    const { value: hideSystemSources, setValue: setHideSystemSources } = useUrlQueryParam('hideSystem', 'true');
    const { value: sourceFilter, setValue: setSourceFilter } = useUrlQueryParam('sourceFilter');
    const { value: searchQuery, setValue: setSearchQuery } = useUrlQueryParam('query');

    // Stable callbacks to prevent infinite re-render loops in child components
    const handleSetSourceFilter = useCallback(
        (value: number | undefined) => setSourceFilter(value?.toString() || ''),
        [setSourceFilter],
    );

    const handleSetHideSystemSources = useCallback(
        (value: boolean) => setHideSystemSources(value.toString()),
        [setHideSystemSources],
    );

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loadedAppConfig && loadedPlatformPrivileges && selectedTab === undefined) {
            const currentPath = location.pathname;

            // Check if current URL matches any tab URL
            const currentTab = Object.entries(tabUrlMap).find(([, url]) => url === currentPath)?.[0] as TabType;

            if (currentTab) {
                // We're on a valid tab URL, set that tab
                setSelectedTab(currentTab);
            } else if (showIngestionTab) {
                // We're not on a tab URL, default to Sources tab
                setSelectedTab(TabType.Sources);
            } else if (showSecretsTab) {
                // We're not on a tab URL, default to Secrets tab
                setSelectedTab(TabType.Secrets);
            } else {
                setSelectedTab(null);
                return;
            }

            // If we're on the base /ingestion path, redirect to the appropriate default tab
            if (currentPath === '/ingestion') {
                const defaultTabType = showIngestionTab ? TabType.Sources : TabType.Secrets;
                history.replace(tabUrlMap[defaultTabType]);
            }
        }
    }, [
        loadedAppConfig,
        loadedPlatformPrivileges,
        showIngestionTab,
        showSecretsTab,
        selectedTab,
        history,
        location.pathname,
    ]);

    const tabs: Tab[] = [
        showIngestionTab && {
            component: (
                <IngestionSourceList
                    showCreateModal={showCreateSourceModal}
                    setShowCreateModal={setShowCreateSourceModal}
                    shouldPreserveParams={shouldPreserveParams}
                    hideSystemSources={hideSystemSources === 'true'}
                    setHideSystemSources={handleSetHideSystemSources}
                    selectedTab={selectedTab}
                    setSelectedTab={setSelectedTab}
                    sourceFilter={sourceFilter ? Number(sourceFilter) : undefined}
                    setSourceFilter={handleSetSourceFilter}
                    searchQuery={searchQuery}
                    setSearchQuery={setSearchQuery}
                />
            ),
            key: TabType.Sources as string,
            name: TabType.Sources as string,
        },
        {
            component: (
                <ExecutionsTab
                    shouldPreserveParams={shouldPreserveParams}
                    hideSystemSources={hideSystemSources === 'true'}
                    setHideSystemSources={handleSetHideSystemSources}
                    selectedTab={selectedTab}
                    setSelectedTab={setSelectedTab}
                />
            ),
            key: TabType.RunHistory as string,
            name: 'Run History',
        },
        showSecretsTab && {
            component: (
                <SecretsList showCreateModal={showCreateSecretModal} setShowCreateModal={setShowCreateSecretModal} />
            ),
            key: TabType.Secrets as string,
            name: TabType.Secrets as string,
        },
    ].filter((tab): tab is Tab => Boolean(tab));

    const onUrlChange = useCallback(
        (tabPath: string) => {
            history.push({ pathname: tabPath, search: '' });
        },
        [history],
    );

    const getCurrentUrl = useCallback(() => location.pathname, [location.pathname]);

    const handleCreateSource = useCallback(() => {
        if (showIngestionOnboardingRedesignV1) {
            analytics.event({
                type: EventType.EnterIngestionFlowEvent,
                entryPoint: 'sources_page_cta',
            });
            history.push(PageRoutes.INGESTION_CREATE);
        } else {
            setShowCreateSourceModal(true);
        }
    }, [showIngestionOnboardingRedesignV1, history]);

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

    if (selectedTab === undefined) {
        return <></>; // loading
    }
    if (selectedTab === null) {
        return <NoPageFound />;
    }

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <OnboardingTour stepIds={[INGESTION_CREATE_SOURCE_ID, INGESTION_REFRESH_SOURCES_ID]} />
            <PageHeaderContainer>
                <TitleContainer>
                    <PageTitle
                        title="Manage Data Sources"
                        subTitle="Configure and schedule syncs to import data from your data sources"
                    />
                </TitleContainer>
                <HeaderActionsContainer>
                    {selectedTab === TabType.Sources && showIngestionTab && (
                        <Tooltip
                            title={
                                !canManageIngestion &&
                                `You don't have permission to perform this action. Please contact your DataHub admin for more info.`
                            }
                        >
                            <div>
                                <Button
                                    variant="filled"
                                    id={INGESTION_CREATE_SOURCE_ID}
                                    onClick={handleCreateSource}
                                    data-testid="create-ingestion-source-button"
                                    icon={{ icon: 'Plus', source: 'phosphor' }}
                                    disabled={!canManageIngestion}
                                >
                                    Create Source
                                </Button>
                            </div>
                        </Tooltip>
                    )}

                    {selectedTab === TabType.Secrets && showSecretsTab && (
                        <Button
                            variant="filled"
                            onClick={handleCreateSecret}
                            data-testid="create-secret-button"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                        >
                            Create secret
                        </Button>
                    )}
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <PageContentContainer>
                <Tabs
                    tabs={tabs}
                    selectedTab={selectedTab}
                    onChange={(tab) => setSelectedTab(tab as TabType)}
                    urlMap={tabUrlMap}
                    onUrlChange={onUrlChange}
                    defaultTab={TabType.Sources}
                    getCurrentUrl={getCurrentUrl}
                />
            </PageContentContainer>
        </PageContainer>
    );
};
