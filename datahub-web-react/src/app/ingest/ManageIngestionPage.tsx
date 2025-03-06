import { Tabs, Typography } from 'antd';
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

enum TabType {
    Sources = 'Sources',
    Secrets = 'Secrets',
    RemoteExecutors = 'Executors',
}

const TabTypeToListComponent = {
    [TabType.Sources]: <IngestionSourceList />,
    [TabType.Secrets]: <SecretsList />,
};

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

    // defaultTab might not be calculated correctly on mount, if `config` or `me` haven't been loaded yet
    useEffect(() => {
        if (loaded && me.loaded && !showIngestionTab && selectedTab === TabType.Sources) {
            setSelectedTab(TabType.Secrets);
        }
    }, [loaded, me.loaded, showIngestionTab, selectedTab]);

    const onClickTab = (newTab: string) => {
        const matchingTab = Object.values(TabType).find((tab) => tab === newTab);
        setSelectedTab(matchingTab || selectedTab);
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
            <StyledTabs activeKey={selectedTab} size="large" onTabClick={(tab: string) => onClickTab(tab)}>
                {showIngestionTab && <Tab key={TabType.Sources} tab={TabType.Sources} />}
                {showSecretsTab && <Tab key={TabType.Secrets} tab={TabType.Secrets} />}
            </StyledTabs>
            <ListContainer>{TabTypeToListComponent[selectedTab]}</ListContainer>
        </PageContainer>
    );
};
