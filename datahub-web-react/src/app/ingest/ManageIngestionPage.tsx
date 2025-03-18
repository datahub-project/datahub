import { Tabs } from 'antd';
import { PageTitle, Button } from '@components';
import { useHistory } from 'react-router';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import { IngestionSourceList } from './source/IngestionSourceList';
import { useAppConfig } from '../useAppConfig';
import { useUserContext } from '../context/useUserContext';
import { SecretsList } from './secret/SecretsList';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { INGESTION_CREATE_SOURCE_ID } from '../onboarding/config/IngestionOnboardingConfig';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { RemoteExecutorPoolsList } from './executor_saas/RemoteExecutorPoolsList';
import { TabType } from './types';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 16px;
    padding-right: 16px;
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

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 20px;
    }
`;

const Tab = styled(Tabs.TabPane)`
    font-size: 14px;
    line-height: 22px;
`;

const ListContainer = styled.div``;

export const ManageIngestionPage = () => {
    /**
     * Determines which view should be visible: ingestion sources or secrets.
     */
    const me = useUserContext();
    const { config, loaded } = useAppConfig();
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const showIngestionTab = isIngestionEnabled && me && me.platformPrivileges?.manageIngestion;
    const showSecretsTab = isIngestionEnabled && me && me.platformPrivileges?.manageSecrets;

    // TODO: For now remote executors privilege is tied to manage ingestion
    const showRemoteExecutorsTab =
        isIngestionEnabled && me && me.platformPrivileges?.manageIngestion && config.featureFlags.displayExecutorPools; // Saas only

    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Sources);
    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

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

    const handleCreateSource = () => {
        setShowCreateSourceModal(true);
    };

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

    const TabTypeToListComponent = {
        [TabType.Sources]: (
            <IngestionSourceList
                onSwitchTab={onSwitchTab}
                showCreateModal={showCreateSourceModal}
                setShowCreateModal={setShowCreateSourceModal}
            />
        ),
        [TabType.Secrets]: (
            <SecretsList showCreateModal={showCreateSecretModal} setShowCreateModal={setShowCreateSecretModal} />
        ),
        // SaaS only
        [TabType.RemoteExecutors]: <RemoteExecutorPoolsList onSwitchTab={onSwitchTab} />,
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
                        >
                            <PlusOutlined style={{ marginRight: '4px' }} /> Create new source
                        </Button>
                    )}

                    {selectedTab === TabType.Secrets && showSecretsTab && (
                        <Button variant="filled" onClick={handleCreateSecret} data-testid="create-secret-button">
                            <PlusOutlined style={{ marginRight: '4px' }} /> Create new secret
                        </Button>
                    )}
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <StyledTabs
                activeKey={selectedTab}
                size="large"
                onTabClick={(tab) => onSwitchTab(tab, { clearQueryParams: true })}
            >
                {showIngestionTab && <Tab key={TabType.Sources} tab={TabType.Sources} />}
                {showSecretsTab && <Tab key={TabType.Secrets} tab={TabType.Secrets} />}
                {/* SaaS only */}
                {showRemoteExecutorsTab && <Tab key={TabType.RemoteExecutors} tab={TabType.RemoteExecutors} />}
            </StyledTabs>
            <ListContainer>{TabTypeToListComponent[selectedTab]}</ListContainer>
        </PageContainer>
    );
};
