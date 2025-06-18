import { Button, PageTitle, Tabs } from '@components';
import React, { useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import { useUserContext } from '@app/context/useUserContext';
import { ExecutionsTab } from '@app/ingestV2/executions/ExecutionsTab';
import { SecretsList } from '@app/ingestV2/secret/SecretsList';
import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import { IngestionSourceList } from '@app/ingestV2/source/IngestionSourceList';
import { TabType, tabUrlMap } from '@app/ingestV2/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    INGESTION_CREATE_SOURCE_ID,
    INGESTION_REFRESH_SOURCES_ID,
} from '@app/onboarding/config/IngestionOnboardingConfig';
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
    const me = useUserContext();
    const { config, loaded } = useAppConfig();
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const showIngestionTab = isIngestionEnabled && me && me.platformPrivileges?.manageIngestion;
    const showSecretsTab = isIngestionEnabled && me && me.platformPrivileges?.manageSecrets;
    const [selectedTab, setSelectedTab] = useState<TabType>();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const [hideSystemSources, setHideSystemSources] = useState(true);

    const {
        isLoading: isCapabilitySummaryLoading,
        error: isCapabilitySummaryError,
        isProfilingSupported,
    } = useCapabilitySummary();

    const history = useHistory();
    const shouldPreserveParams = useRef(false);

    if (!isCapabilitySummaryLoading && !isCapabilitySummaryError) {
        console.log(
            'Example to be removed when is actually used for something is profiling support for bigquery',
            isProfilingSupported('bigquery'),
        );
    }

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loaded && me.loaded && !showIngestionTab && selectedTab === TabType.Sources) {
            setSelectedTab(TabType.Secrets);
        }
    }, [loaded, me.loaded, showIngestionTab, selectedTab]);

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
                    selectedTab={selectedTab}
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

    const onUrlChange = (tabPath: string) => {
        history.push(tabPath);
    };

    const handleCreateSource = () => {
        setShowCreateSourceModal(true);
    };

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

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
                        <div>
                            <Button
                                variant="filled"
                                id={INGESTION_CREATE_SOURCE_ID}
                                onClick={handleCreateSource}
                                data-testid="create-ingestion-source-button"
                                icon={{ icon: 'Plus', source: 'phosphor' }}
                            >
                                Create source
                            </Button>
                        </div>
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
