import { Tabs } from 'antd';
import { PageTitle, Button } from '@components';
import React, { useEffect, useState, useCallback } from 'react';
import styled from 'styled-components';
import { Plus } from 'phosphor-react';
import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { IngestionSourceList } from './source/IngestionSourceList';
import { useAppConfig } from '../useAppConfig';
import { useUserContext } from '../context/useUserContext';
import { SecretsList } from './secret/SecretsList';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { INGESTION_CREATE_SOURCE_ID } from '../onboarding/config/IngestionOnboardingConfig';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding: 16px 20px;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    height: 100%;
    display: flex;
    flex-direction: column;
`;

const PageHeaderContainer = styled.div`
    && {
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
    }
`;

enum TabType {
    Sources = 'Sources',
    Secrets = 'Secrets',
}

export const ManageIngestionPage = () => {
    const { config } = useAppConfig();
    const me = useUserContext();
    const location = useLocation();
    const history = useHistory();
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
    const showIngestionTab = isIngestionEnabled && me && me.platformPrivileges?.manageIngestion;
    const showSecretsTab = isIngestionEnabled && me && me.platformPrivileges?.manageSecrets;

    // Get initial tab from URL or default to Sources
    const params = QueryString.parse(location.search);
    const [selectedTab, setSelectedTab] = useState<TabType>((params.tab as TabType) || TabType.Sources);

    // Update URL when tab changes
    const onSwitchTab = useCallback(
        (newTab: string) => {
            const newParams = { ...params, tab: newTab };
            history.replace({
                pathname: location.pathname,
                search: QueryString.stringify(newParams),
            });
            setSelectedTab(newTab as TabType);
        },
        [history, location.pathname, params],
    );

    // Set default tab based on permissions
    useEffect(() => {
        if (config && me) {
            if (!showIngestionTab && showSecretsTab) {
                onSwitchTab(TabType.Secrets);
            }
        }
    }, [config, me, showIngestionTab, showSecretsTab, onSwitchTab]);

    const [showCreateSourceModal, setShowCreateSourceModal] = useState<boolean>(false);
    const [showCreateSecretModal, setShowCreateSecretModal] = useState<boolean>(false);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const handleCreateSource = () => {
        setShowCreateSourceModal(true);
    };

    const handleCreateSecret = () => {
        setShowCreateSecretModal(true);
    };

    const TabTypeToListComponent = {
        [TabType.Sources]: showIngestionTab ? (
            <IngestionSourceList
                _onSwitchTab={onSwitchTab}
                showCreateModal={showCreateSourceModal}
                setShowCreateModal={setShowCreateSourceModal}
            />
        ) : null,
        [TabType.Secrets]: showSecretsTab ? (
            <SecretsList showCreateModal={showCreateSecretModal} setShowCreateModal={setShowCreateSecretModal} />
        ) : null,
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
                            <Plus style={{ marginRight: '4px' }} /> Create new source
                        </Button>
                    )}

                    {selectedTab === TabType.Secrets && showSecretsTab && (
                        <Button variant="filled" onClick={handleCreateSecret} data-testid="create-secret-button">
                            <Plus style={{ marginRight: '4px' }} /> Create new secret
                        </Button>
                    )}
                </HeaderActionsContainer>
            </PageHeaderContainer>
            <StyledTabs
                activeKey={selectedTab}
                onChange={onSwitchTab}
                items={[
                    {
                        key: TabType.Sources,
                        label: 'Sources',
                        disabled: !showIngestionTab,
                    },
                    {
                        key: TabType.Secrets,
                        label: 'Secrets',
                        disabled: !showSecretsTab,
                    },
                ]}
            />
            {TabTypeToListComponent[selectedTab]}
        </PageContainer>
    );
};
