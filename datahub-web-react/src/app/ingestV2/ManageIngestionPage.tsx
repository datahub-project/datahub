import { Button, PageTitle, Tabs } from '@components';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import { useUserContext } from '@app/context/useUserContext';
import { ExecutionsTab } from '@app/ingestV2/executions/ExecutionsTab';
import { SecretsList } from '@app/ingestV2/secret/SecretsList';
import { IngestionSourceList } from '@app/ingestV2/source/IngestionSourceList';
import { TabType } from '@app/ingestV2/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { INGESTION_CREATE_SOURCE_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
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
    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Sources);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loaded && me.loaded && !showIngestionTab && selectedTab === TabType.Sources) {
            setSelectedTab(TabType.Secrets);
        }
    }, [loaded, me.loaded, showIngestionTab, selectedTab]);

    const history = useHistory();
    const onSwitchTab = (newTab: string, options?: { clearQueryParams: boolean }) => {
        const matchingTab = Object.values(TabType).find((tab) => tab === newTab);
        setSelectedTab(matchingTab || selectedTab);
        if (options?.clearQueryParams) {
            history.push({ search: '' });
        }
    };

    const tabs: Tab[] = [
        showIngestionTab && {
            component: (
                <IngestionSourceList
                    showCreateModal={showCreateSourceModal}
                    setShowCreateModal={setShowCreateSourceModal}
                />
            ),
            key: TabType.Sources as string,
            name: TabType.Sources as string,
        },
        {
            component: <ExecutionsTab />,
            key: TabType.ExecutionLog as string,
            name: 'Execution Log',
        },
        showSecretsTab && {
            component: (
                <SecretsList showCreateModal={showCreateSecretModal} setShowCreateModal={setShowCreateSecretModal} />
            ),
            key: TabType.Secrets as string,
            name: TabType.Secrets as string,
        },
    ].filter((tab): tab is Tab => Boolean(tab));

    const handleCreateSource = () => {
        setShowCreateSourceModal(true);
    };

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <OnboardingTour stepIds={[INGESTION_CREATE_SOURCE_ID]} />
            <PageHeaderContainer>
                <TitleContainer>
                    <PageTitle
                        title="Manage Data Sources"
                        subTitle="Configure and schedule syncs to import data from your data sources"
                    />
                </TitleContainer>
                <HeaderActionsContainer>
                    {selectedTab === TabType.Sources && showIngestionTab && (
                        <Button
                            variant="filled"
                            id={INGESTION_CREATE_SOURCE_ID}
                            onClick={handleCreateSource}
                            data-testid="create-ingestion-source-button"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                        >
                            Create new source
                        </Button>
                    )}

                    {selectedTab === TabType.Secrets && showSecretsTab && (
                        <Button
                            variant="filled"
                            onClick={handleCreateSecret}
                            data-testid="create-secret-button"
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                        >
                            Create new secret
                        </Button>
                    )}
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <PageContentContainer>
                <Tabs
                    tabs={tabs}
                    selectedTab={selectedTab}
                    onChange={(tab) => onSwitchTab(tab, { clearQueryParams: true })}
                />
            </PageContentContainer>
        </PageContainer>
    );
};
