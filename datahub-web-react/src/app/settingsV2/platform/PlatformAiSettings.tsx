import React, { useCallback, useEffect, useState } from 'react';
import { Route, Switch, useHistory, useLocation, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import {
    AI_TAB_URL_MAP,
    AiTabType,
    DEFAULT_AI_TAB,
    determineActiveTabFromUrl,
} from '@app/settingsV2/platform/PlatformAiSettings.types';
import { AiSettingsTab } from '@app/settingsV2/platform/ai/AiSettingsTab';
import { AiPluginsTab } from '@app/settingsV2/platform/ai/plugins/AiPluginsTab';
import { useAppConfig } from '@app/useAppConfig';
import { Button, PageTitle, Tabs } from '@src/alchemy-components';

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
    const history = useHistory();
    const location = useLocation();
    const { path } = useRouteMatch();
    const { config } = useAppConfig();

    const [selectedTab, setSelectedTab] = useState<AiTabType | undefined>();
    const [showCreatePluginModal, setShowCreatePluginModal] = useState(false);

    // Determine active tab from URL
    useEffect(() => {
        if (selectedTab === undefined) {
            const result = determineActiveTabFromUrl(location.pathname);

            if (result.tab) {
                setSelectedTab(result.tab);
            }

            if (result.shouldRedirect && result.redirectUrl) {
                history.replace(result.redirectUrl);
            }
        }
    }, [selectedTab, location.pathname, history]);

    // Check for create modal query param and open modal
    useEffect(() => {
        const searchParams = new URLSearchParams(location.search);
        if (searchParams.get('create') === 'true' && selectedTab === AiTabType.Plugins) {
            setShowCreatePluginModal(true);
            // Clean up URL by removing query param
            searchParams.delete('create');
            const newSearch = searchParams.toString();
            const newUrl = newSearch ? `${location.pathname}?${newSearch}` : location.pathname;
            history.replace(newUrl);
        }
    }, [location.search, location.pathname, selectedTab, history]);

    const handleTabChange = useCallback((tab: string) => {
        setSelectedTab(tab as AiTabType);
    }, []);

    const onUrlChange = useCallback(
        (tabPath: string) => {
            history.push(tabPath);
        },
        [history],
    );

    const getCurrentUrl = useCallback(() => location.pathname, [location.pathname]);

    // Check if AI Plugins feature flag is enabled
    const showAiPluginsTab = config.featureFlags.aiPluginsEnabled;

    const tabs: Tab[] = [
        {
            name: 'Settings',
            key: AiTabType.Settings,
            component: <AiSettingsTab />,
        },
        // Only show Plugins tab if feature flag is enabled
        ...(showAiPluginsTab
            ? [
                  {
                      name: 'Plugins',
                      key: AiTabType.Plugins,
                      component: (
                          <AiPluginsTab
                              showCreateModal={showCreatePluginModal}
                              setShowCreateModal={setShowCreatePluginModal}
                          />
                      ),
                  },
              ]
            : []),
    ];

    // Dynamic title and subtitle based on selected tab
    const pageTitle = selectedTab === AiTabType.Plugins ? 'Ask DataHub Plugins' : 'AI';
    const pageSubtitle =
        selectedTab === AiTabType.Plugins
            ? 'Configure plugins to add capabilities to Ask DataHub chat assistant. Once configured, plugins will be visible to all users.'
            : 'Configure AI-powered features';

    return (
        <PageContainer>
            <Switch>
                {/* Main tabs view */}
                <Route path={path}>
                    <PageHeaderContainer>
                        <TitleContainer>
                            <PageTitle title={pageTitle} subTitle={pageSubtitle} />
                        </TitleContainer>
                        <HeaderActionsContainer>
                            {selectedTab === AiTabType.Plugins && (
                                <Button
                                    variant="filled"
                                    onClick={() => setShowCreatePluginModal(true)}
                                    data-testid="create-ai-plugin-button"
                                    icon={{ icon: 'Plus', source: 'phosphor' }}
                                >
                                    Create
                                </Button>
                            )}
                        </HeaderActionsContainer>
                    </PageHeaderContainer>
                    <TabsContainer>
                        <Tabs
                            tabs={tabs}
                            selectedTab={selectedTab}
                            onChange={handleTabChange}
                            urlMap={AI_TAB_URL_MAP}
                            onUrlChange={onUrlChange}
                            defaultTab={DEFAULT_AI_TAB}
                            getCurrentUrl={getCurrentUrl}
                        />
                    </TabsContainer>
                </Route>
            </Switch>
        </PageContainer>
    );
};
