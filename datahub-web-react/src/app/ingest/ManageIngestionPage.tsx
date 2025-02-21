import { Tabs, Typography } from 'antd';
import { useHistory } from 'react-router';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { IngestionSourceList } from './source/IngestionSourceList';
import { useAppConfig } from '../useAppConfig';
import { useUserContext } from '../context/useUserContext';
import { SecretsList } from './secret/SecretsList';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    INGESTION_CREATE_SOURCE_ID,
    INGESTION_REFRESH_SOURCES_ID,
} from '../onboarding/config/IngestionOnboardingConfig';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { RemoteExecutorPoolsList } from './executor_saas/RemoteExecutorPoolsList';
import { TabType } from './source/types';

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

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const StyledTabs = styled(Tabs)`
    &&& .ant-tabs-nav {
        margin-bottom: 0;
        padding-left: 28px;
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
    const showRemoteExecutorsTab = isIngestionEnabled && me && me.platformPrivileges?.manageIngestion; // Saas only

    const [selectedTab, setSelectedTab] = useState<TabType>(TabType.Sources);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const history = useHistory();

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loaded && me.loaded && !showIngestionTab && selectedTab === TabType.Sources) {
            setSelectedTab(TabType.Secrets);
        }
    }, [loaded, me.loaded, showIngestionTab, selectedTab]);

    const onSwitchTab = (newTab: string, options?: { clearQueryParams: boolean }) => {
        const matchingTab = Object.values(TabType).find((tab) => tab === newTab);
        setSelectedTab(matchingTab || selectedTab);
        if (options?.clearQueryParams) {
            history.push({ search: '' });
        }
    };

    const TabTypeToListComponent = {
        [TabType.Sources]: <IngestionSourceList onSwitchTab={onSwitchTab} />,
        [TabType.Secrets]: <SecretsList />,
        // SaaS only
        [TabType.RemoteExecutors]: <RemoteExecutorPoolsList onSwitchTab={onSwitchTab} />,
    };
    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <OnboardingTour stepIds={[INGESTION_CREATE_SOURCE_ID, INGESTION_REFRESH_SOURCES_ID]} />
            <PageHeaderContainer>
                <PageTitle level={3}>Manage Data Sources</PageTitle>
                <Typography.Paragraph type="secondary">
                    Configure and schedule syncs to import data from your data sources
                </Typography.Paragraph>
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
