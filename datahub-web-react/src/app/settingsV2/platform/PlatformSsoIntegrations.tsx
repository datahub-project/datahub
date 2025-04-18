import { Divider, List, Typography } from 'antd';
import React from 'react';
import { Route, Switch, useHistory, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import { PlatformIntegrationItem } from '@app/settingsV2/platform/PlatformIntegrationCard';
import { SUPPORTED_SSO_INTEGRATIONS } from '@app/settingsV2/platform/types';

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

export const PlatformSsoIntegrations = () => {
    const history = useHistory();
    const { path } = useRouteMatch();

    const selectIntegration = (id: string) => {
        history.push(`/settings/sso/${id}`);
    };

    return (
        <Page>
            <Switch>
                <Route exact path={path}>
                    <ContentContainer>
                        <Typography.Title level={3}>SSO Integrations</Typography.Title>
                        <Typography.Text type="secondary">Connect DataHub to your SSO provider</Typography.Text>
                        <Divider />
                        <List
                            dataSource={SUPPORTED_SSO_INTEGRATIONS}
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
                {SUPPORTED_SSO_INTEGRATIONS.map((i) => (
                    <Route path={`${path}/${i.id}`} render={() => i.content} key={i.id} />
                ))}
            </Switch>
        </Page>
    );
};
