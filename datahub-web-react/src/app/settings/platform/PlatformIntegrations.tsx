import { Divider, List, Typography } from 'antd';
import React from 'react';
import { Route, Switch, useHistory, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import { PlatformIntegrationItem } from '@app/settings/platform/PlatformIntegrationCard';
import { SUPPORTED_INTEGRATIONS } from '@app/settings/platform/types';
import { useAppConfig } from '@app/useAppConfig';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const ContentContainer = styled.div`
    padding-top: 40px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

export const PlatformIntegrations = () => {
    const history = useHistory();
    const { path } = useRouteMatch();
    const { config } = useAppConfig();

    const selectIntegration = (id: string) => {
        history.push(`/settings/integrations/${id}`);
    };

    // Filter out Teams integration if the feature flag is disabled
    const visibleIntegrations = SUPPORTED_INTEGRATIONS.filter((integration) => {
        if (integration.id === 'microsoft-teams') {
            return config.featureFlags.teamsNotificationsEnabled;
        }
        return true;
    });

    return (
        <Page>
            <Switch>
                <Route exact path={path}>
                    <ContentContainer>
                        <Typography.Title level={3}>Integrations</Typography.Title>
                        <Typography.Text type="secondary">Manage integrations with third party tools</Typography.Text>
                        <Divider />
                        <List
                            dataSource={visibleIntegrations}
                            split
                            renderItem={(integration) => (
                                <PlatformIntegrationItem
                                    name={integration.name}
                                    description={integration.description}
                                    img={integration.img}
                                    onClick={() => selectIntegration(integration.id)}
                                />
                            )}
                        />
                    </ContentContainer>
                </Route>
                {visibleIntegrations.map((i) => (
                    <Route path={`${path}/${i.id}`} render={() => i.content} key={i.id} />
                ))}
            </Switch>
        </Page>
    );
};
