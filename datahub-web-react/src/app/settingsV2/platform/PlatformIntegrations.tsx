import React, { useCallback, useEffect, useState } from 'react';
import { Route, Switch, useHistory, useLocation, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import { Tab } from '@components/components/Tabs/Tabs';

import { PlatformIntegrationItem } from '@app/settingsV2/platform/PlatformIntegrationCard';
import {
    DEFAULT_INTEGRATIONS_TAB,
    INTEGRATIONS_TAB_URL_MAP,
    IntegrationsTabType,
    determineActiveTabFromUrl,
    filterIntegrationsByFeatureFlags,
    isIntegrationDetailPage,
} from '@app/settingsV2/platform/PlatformIntegrations.types';
import { AiPluginsTab } from '@app/settingsV2/platform/aiPlugins/AiPluginsTab';
import { DATA_INTEGRATIONS, NOTIFICATION_INTEGRATIONS } from '@app/settingsV2/platform/types';
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
        overflow: hidden;
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

const IntegrationsList = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

type IntegrationItem = {
    id: string;
    name: string;
    description: string;
    img: string;
    content: React.ReactNode;
};

type IntegrationsListContentProps = {
    integrations: IntegrationItem[];
    onSelectIntegration: (id: string) => void;
};

const IntegrationsListContent = ({ integrations, onSelectIntegration }: IntegrationsListContentProps) => {
    return (
        <IntegrationsList>
            {integrations.map((integration) => (
                <PlatformIntegrationItem
                    key={integration.id}
                    name={integration.name}
                    description={integration.description}
                    img={integration.img}
                    onClick={() => onSelectIntegration(integration.id)}
                />
            ))}
        </IntegrationsList>
    );
};

export const PlatformIntegrations = () => {
    const history = useHistory();
    const location = useLocation();
    const { path } = useRouteMatch();
    const { config } = useAppConfig();

    const [selectedTab, setSelectedTab] = useState<IntegrationsTabType | undefined>();
    const [showCreatePluginModal, setShowCreatePluginModal] = useState(false);

    // Filter integrations based on feature flags
    const notificationIntegrations = filterIntegrationsByFeatureFlags(NOTIFICATION_INTEGRATIONS, config.featureFlags);

    // All integrations for detail page routing
    const allIntegrations = [...notificationIntegrations, ...DATA_INTEGRATIONS];

    const selectIntegration = useCallback(
        (id: string) => {
            history.push(`/settings/integrations/${id}`);
        },
        [history],
    );

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

    const handleTabChange = useCallback((tab: string) => {
        setSelectedTab(tab as IntegrationsTabType);
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
            name: 'Notifications',
            key: IntegrationsTabType.Notifications,
            component: (
                <IntegrationsListContent
                    integrations={notificationIntegrations}
                    onSelectIntegration={selectIntegration}
                />
            ),
        },
        {
            name: 'Integrations',
            key: IntegrationsTabType.DataIntegrations,
            component: (
                <IntegrationsListContent integrations={DATA_INTEGRATIONS} onSelectIntegration={selectIntegration} />
            ),
        },
        // Only show AI Plugins tab if feature flag is enabled
        ...(showAiPluginsTab
            ? [
                  {
                      name: 'AI Plugins',
                      key: IntegrationsTabType.AiPlugins,
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

    // Check if we're on an integration detail page
    const isDetailPage = isIntegrationDetailPage(location.pathname, allIntegrations);

    return (
        <PageContainer>
            <Switch>
                {/* Integration detail pages */}
                {allIntegrations.map((integration) => (
                    <Route path={`${path}/${integration.id}`} render={() => integration.content} key={integration.id} />
                ))}

                {/* Main tabs view */}
                <Route path={path}>
                    {!isDetailPage && (
                        <>
                            <PageHeaderContainer>
                                <TitleContainer>
                                    <PageTitle
                                        title="Integrations"
                                        subTitle="Manage integrations with third party tools and services"
                                    />
                                </TitleContainer>
                                <HeaderActionsContainer>
                                    {selectedTab === IntegrationsTabType.AiPlugins && (
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
                                    urlMap={INTEGRATIONS_TAB_URL_MAP}
                                    onUrlChange={onUrlChange}
                                    defaultTab={DEFAULT_INTEGRATIONS_TAB}
                                    getCurrentUrl={getCurrentUrl}
                                />
                            </TabsContainer>
                        </>
                    )}
                </Route>
            </Switch>
        </PageContainer>
    );
};
