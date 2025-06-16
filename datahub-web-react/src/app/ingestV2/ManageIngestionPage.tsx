import { Button, PageTitle, Tabs, Tooltip } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import { useUserContext } from '@app/context/useUserContext';
import { ExecutionsTab } from '@app/ingestV2/executions/ExecutionsTab';
import {
    REMOTE_EXECUTORS_CREATE_SOURCE_ID,
    RemoteExecutorPoolsList,
} from '@app/ingestV2/executor_saas/RemoteExecutorPoolsList';
import { SecretsList } from '@app/ingestV2/secret/SecretsList';
import { IngestionSourceList } from '@app/ingestV2/source/IngestionSourceList';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    INGESTION_CREATE_SOURCE_ID,
    INGESTION_REFRESH_SOURCES_ID,
} from '@app/onboarding/config/IngestionOnboardingConfig';
import { NoPageFound } from '@app/shared/NoPageFound';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

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
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const canViewIngestionPage =
        platformPrivileges?.canViewIngestionPage && config.featureFlags.viewIngestionSourcePrivilegesEnabled;

    const canManageIngestion = platformPrivileges?.manageIngestion;
    const showIngestionTab = isIngestionEnabled && (canManageIngestion || canViewIngestionPage);
    const showSecretsTab = isIngestionEnabled && platformPrivileges?.manageSecrets;
    const canManagePools = canManageIngestion;
    const canViewPools = canViewIngestionPage;
    // TODO: For now remote executors privilege is tied to manage ingestion
    const showRemoteExecutorsTab = showIngestionTab && config.featureFlags.displayExecutorPools; // Saas only

    // undefined == not loaded, null == no permissions
    const [selectedTab, setSelectedTab] = useState<TabType | undefined | null>();

    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const [hideSystemSources, setHideSystemSources] = useState(true);
    const [showCreatePoolModal, setShowCreatePoolModal] = useState(false); // SaaS-only

    const history = useHistory();
    const shouldPreserveParams = useRef(false);

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loadedAppConfig && loadedPlatformPrivileges && selectedTab === undefined) {
            if (showIngestionTab) {
                setSelectedTab(TabType.Sources);
            } else if (showSecretsTab) {
                setSelectedTab(TabType.Secrets);
            } else if (showRemoteExecutorsTab) {
                setSelectedTab(TabType.RemoteExecutors);
            } else {
                setSelectedTab(null);
            }
        }
    }, [
        loadedAppConfig,
        loadedPlatformPrivileges,
        showIngestionTab,
        showSecretsTab,
        showRemoteExecutorsTab,
        selectedTab,
    ]);

    const onSwitchTab = (newTab: string, options?: { clearQueryParams: boolean }) => {
        const preserveParams = shouldPreserveParams.current;
        const matchingTab = Object.values(TabType).find((tab) => tab === newTab);
        if (!preserveParams && options?.clearQueryParams) {
            history.push({ search: '' });
        }
        setSelectedTab(matchingTab || selectedTab);
        shouldPreserveParams.current = false;
    };

    const tabs: Tab[] = [
        showIngestionTab && {
            component: (
                <IngestionSourceList
                    showCreateModal={showCreateSourceModal}
                    setShowCreateModal={setShowCreateSourceModal}
                    shouldPreserveParams={shouldPreserveParams}
                    hideSystemSources={hideSystemSources}
                    setHideSystemSources={setHideSystemSources}
                    setSelectedTab={setSelectedTab}
                />
            ),
            key: TabType.Sources as string,
            name: TabType.Sources as string,
        },
        {
            component: (
                <ExecutionsTab
                    shouldPreserveParams={shouldPreserveParams}
                    hideSystemSources={hideSystemSources}
                    setHideSystemSources={setHideSystemSources}
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
        // SaaS only
        showRemoteExecutorsTab &&
            canViewPools && {
                component: (
                    <RemoteExecutorPoolsList
                        showCreatePoolModal={showCreatePoolModal}
                        setShowCreatePoolModal={setShowCreatePoolModal}
                        shouldPreserveParams={shouldPreserveParams}
                    />
                ),
                key: TabType.RemoteExecutors as string,
                name: TabType.RemoteExecutors as string,
            },
    ].filter((tab): tab is Tab => Boolean(tab));

    const onUrlChange = (tabPath: string) => {
        history.push(tabPath);
    };

    const handleCreateSource = () => {
        setShowCreateSourceModal(true);
    };

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

    const handleCreatePool = () => {
        setShowCreatePoolModal(true);
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
                                    Create source
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
                    {selectedTab === TabType.RemoteExecutors && showRemoteExecutorsTab && canViewPools && (
                        <Button
                            variant="filled"
                            onClick={handleCreatePool}
                            data-testid="create-pool-button"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                            id={REMOTE_EXECUTORS_CREATE_SOURCE_ID}
                            disabled={!canManagePools}
                        >
                            Create pool
                        </Button>
                    )}
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <PageContentContainer>
                <Tabs
                    tabs={tabs}
                    selectedTab={selectedTab}
                    onChange={(tab) => onSwitchTab(tab, { clearQueryParams: true })}
                    urlMap={tabUrlMap}
                    onUrlChange={onUrlChange}
                    defaultTab={TabType.Sources}
                    getCurrentUrl={() => window.location.pathname}
                />
            </PageContentContainer>
        </PageContainer>
    );
};
