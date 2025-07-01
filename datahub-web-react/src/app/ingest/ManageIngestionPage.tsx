import { PlusOutlined } from '@ant-design/icons';
import { Button, PageTitle } from '@components';
import { Tabs } from 'antd';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { SecretsList } from '@app/ingest/secret/SecretsList';
import { IngestionSourceList } from '@app/ingest/source/IngestionSourceList';
import { TabType } from '@app/ingest/types';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { INGESTION_CREATE_SOURCE_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import { NoPageFound } from '@app/shared/NoPageFound';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 16px;
    padding-right: 16px;
    background-color: white;
    height: 100%;
    display: flex;
    flex-direction: column;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
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

const ListContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

export const ManageIngestionPage = () => {
    /**
     * Determines which view should be visible: ingestion sources or secrets.
     */
    const { platformPrivileges, loaded: loadedPlatformPrivileges } = useUserContext();
    const { config, loaded: loadedAppConfig } = useAppConfig();
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const showIngestionTab = isIngestionEnabled && platformPrivileges?.manageIngestion;
    const showSecretsTab = isIngestionEnabled && platformPrivileges?.manageSecrets;

    // undefined == not loaded, null == no permissions
    const [selectedTab, setSelectedTab] = useState<TabType | undefined | null>();

    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loadedAppConfig && loadedPlatformPrivileges && selectedTab === undefined) {
            if (showIngestionTab) {
                setSelectedTab(TabType.Sources);
            } else if (showSecretsTab) {
                setSelectedTab(TabType.Secrets);
            } else {
                setSelectedTab(null);
            }
        }
    }, [loadedAppConfig, loadedPlatformPrivileges, showIngestionTab, showSecretsTab, selectedTab]);

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
                showCreateModal={showCreateSourceModal}
                setShowCreateModal={setShowCreateSourceModal}
            />
        ),
        [TabType.Secrets]: (
            <SecretsList showCreateModal={showCreateSecretModal} setShowCreateModal={setShowCreateSecretModal} />
        ),
    };

    if (selectedTab === undefined) {
        return <></>; // loading
    }
    if (selectedTab === null) {
        return <NoPageFound />;
    }

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
            </StyledTabs>
            <ListContainer>{TabTypeToListComponent[selectedTab]}</ListContainer>
        </PageContainer>
    );
};
