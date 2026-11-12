import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
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
import { useAppConfig } from '@app/useAppConfig';
import { Button, PageTitle } from '@src/alchemy-components';

import { useGetMcpServersQuery } from '@graphql/mcpServers.generated';

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
    const { t: tc } = useTranslation('common.actions');
    const { config } = useAppConfig();
    const [showCreateMcpServerModal, setShowCreateMcpServerModal] = useState(false);

    // Read the MCP master switch so Create hides while MCP is off. Apollo dedupes this with
    // the tab's identical query.
    const { data: mcpData, error: mcpError } = useGetMcpServersQuery({
        skip: !canManageMcpServers,
        errorPolicy: 'all',
    });
    const scopedServersEnabled = config.featureFlags.createMcpServersEnabled;
    // The tab shows an error instead of the list, and a create can't check slugs it never loaded.
    const canCreateMcpServers =
        scopedServersEnabled && (mcpData?.mcpSettings?.enabled ?? true) && !(mcpError && !mcpData?.mcpSettings);

    const mcpServersTab: AiSettingsTab | null = canManageMcpServers
        ? {
              key: AiTabType.McpServers,
              // Without scoped servers DataHub serves a single MCP server, so the tab is named for one.
              name: t(scopedServersEnabled ? 'platform.ai.mcpServers.tabTitle' : 'platform.ai.mcpServer.tabTitle'),
              title: t(scopedServersEnabled ? 'platform.ai.mcpServers.pageTitle' : 'platform.ai.mcpServer.pageTitle'),
              subtitle: t('platform.ai.mcpServers.pageSubtitle'),
              component: (
                  <McpServersTab
                      showCreateModal={showCreateMcpServerModal}
                      setShowCreateModal={setShowCreateMcpServerModal}
                  />
              ),
              headerActions: canCreateMcpServers ? (
                  <Button
                      variant="filled"
                      onClick={() => setShowCreateMcpServerModal(true)}
                      data-testid="create-mcp-server-button"
                      icon={{ icon: Plus }}
                  >
                      {tc('create')}
                  </Button>
              ) : undefined,
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
