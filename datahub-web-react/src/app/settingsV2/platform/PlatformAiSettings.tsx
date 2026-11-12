import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect, useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import {
    AiSettingsTab,
    AiTabType,
    aiTabUrl,
    getAiTabFromPath,
    isAiSettingsBasePath,
    sortAiSettingsTabs,
} from '@app/settingsV2/platform/PlatformAiSettings.types';
import { useAdditionalAiSettingsTabs } from '@app/settingsV2/platform/additionalAiSettingsTabs';
import { McpServersTab } from '@app/settingsV2/platform/ai/mcpServers/McpServersTab';
import { useCanManageMcpServers } from '@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers';
import { Tabs } from '@app/sharedV2/tabs/Tabs';
import { PageTitle } from '@src/alchemy-components';

// Where MCP Servers sits among the tabs; deployment tabs order themselves around it.
const MCP_SERVERS_TAB_ORDER = 20;

const PageContainer = styled.div`
    width: 100%;
    height: 100%;
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    overflow: hidden;
`;

const PageHeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    flex-shrink: 0;
`;

const TitleContainer = styled.div`
    flex: 1;
`;

const HeaderActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

const TabsContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    min-height: 0;

    .ant-tabs {
        height: 100%;
        display: flex;
        flex-direction: column;
    }

    .ant-tabs-content-holder {
        flex: 1;
        overflow: clip;
        min-height: 0;
    }

    .ant-tabs-content {
        height: 100%;
    }

    .ant-tabs-tabpane-active {
        height: 100%;
        display: flex;
        flex-direction: column;
    }
`;

export const PlatformAiSettings = () => {
    const { t } = useTranslation('settings.ai');
    const history = useHistory();
    const { pathname } = useLocation();
    const canManageMcpServers = useCanManageMcpServers();
    const additionalTabs = useAdditionalAiSettingsTabs();

    const mcpServersTab: AiSettingsTab | null = canManageMcpServers
        ? {
              key: AiTabType.McpServers,
              name: t('platform.ai.mcpServers.tabTitle'),
              title: t('platform.ai.mcpServers.pageTitle'),
              subtitle: t('platform.ai.mcpServers.pageSubtitle'),
              component: <McpServersTab />,
              order: MCP_SERVERS_TAB_ORDER,
          }
        : null;

    const aiTabs = sortAiSettingsTabs([...(mcpServersTab ? [mcpServersTab] : []), ...additionalTabs]);
    const tabKeys = aiTabs.map((tab) => tab.key);
    const urlMap = Object.fromEntries(tabKeys.map((key) => [key, aiTabUrl(key)]));
    const selectedKey = getAiTabFromPath(pathname, tabKeys);
    const selectedTab = aiTabs.find((tab) => tab.key === selectedKey);

    const onUrlChange = useCallback((tabPath: string) => history.push(tabPath), [history]);

    // Map sub-paths (/settings/ai/<tab>/...) back to their tab URL so the Tabs component's
    // URL matching doesn't fall through to the default tab and navigate away.
    const getCurrentUrl = useMemo(() => {
        const url = selectedKey ? aiTabUrl(selectedKey) : pathname;
        return () => url;
    }, [selectedKey, pathname]);

    const firstTab = aiTabs[0];
    // Nothing to show: the MCP server is off (MCP_SERVER_ENABLED) or the user can't manage it.
    if (!firstTab) {
        return <Redirect to="/settings" />;
    }
    if (isAiSettingsBasePath(pathname)) {
        return <Redirect to={aiTabUrl(firstTab.key)} />;
    }

    const tabs: Tab[] = aiTabs.map((tab) => ({ key: tab.key, name: tab.name, component: tab.component }));

    return (
        <PageContainer>
            <PageHeaderContainer>
                <TitleContainer>
                    <PageTitle
                        title={selectedTab?.title ?? t('platform.ai.navTitle')}
                        subTitle={selectedTab?.subtitle}
                    />
                </TitleContainer>
                {selectedTab?.headerActions && (
                    <HeaderActionsContainer>{selectedTab.headerActions}</HeaderActionsContainer>
                )}
            </PageHeaderContainer>
            {tabs.length > 0 && (
                <TabsContainer>
                    <Tabs
                        tabs={tabs}
                        selectedTab={selectedKey}
                        urlMap={urlMap}
                        onUrlChange={onUrlChange}
                        defaultTab={firstTab?.key}
                        getCurrentUrl={getCurrentUrl}
                    />
                </TabsContainer>
            )}
        </PageContainer>
    );
};
