import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Button, Divider, Form, Input, Typography } from 'antd';
import { useConnectionQuery, useUpsertConnectionMutation } from '../../../../graphql/connection.generated';
import { DataHubConnectionDetailsType } from '../../../../types.generated';
import { PlatformIntegrationBreadcrumb } from '../PlatformIntegrationBreadcrumb';

import {
    TEAMS_CONNECTION_ID,
    TEAMS_CONNECTION_URN,
    TEAMS_PLATFORM_URN,
    getTeamsConnection,
    getWebhookURL,
} from './utils';
import { ToastType, showToastMessage } from '../../../sharedV2/toastMessageUtils';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const ContentContainer = styled.div`
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    width: 100%;
`;

const Content = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;
`;

const FormColumn = styled.div``;

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

export const TeamsIntegration = () => {
    const [webhookURL, setWebhookURL] = useState<string>('');

    const [upsertConnection] = useUpsertConnectionMutation();

    const { data: connectionData } = useConnectionQuery({
        variables: {
            urn: TEAMS_CONNECTION_URN,
        },
    });

    useEffect(() => {
        const existingJson = connectionData?.connection?.details?.json;
        const url = existingJson ? getWebhookURL(existingJson.blob) : '';
        setWebhookURL(url);
    }, [connectionData]);

    const connectToTeams = () => {
        upsertConnection({
            variables: {
                input: {
                    id: TEAMS_CONNECTION_ID,
                    platformUrn: TEAMS_PLATFORM_URN,
                    type: DataHubConnectionDetailsType.Json,
                    json: {
                        blob: getTeamsConnection(webhookURL),
                    },
                },
            },
        })
            .then(() => {
                showToastMessage(ToastType.SUCCESS, 'Successfully updated your Teams connection', 3);
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to update your Teams connection', 3);
            });
    };

    return (
        <Page>
            <ContentContainer>
                <PlatformIntegrationBreadcrumb name="Teams" />
                <Typography.Title level={3}>Teams</Typography.Title>
                <Typography.Text type="secondary">Configure an integration with Teams</Typography.Text>
                <Divider />
                <Content>
                    <FormColumn>
                        <Form layout="vertical">
                            <Form.Item label={<Typography.Text strong>Notifications Channel</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Enter the webhook url for the channel you want notifications to be sent to.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={webhookURL}
                                        data-testid="webhook-url-input"
                                        onChange={(e) => setWebhookURL(e.target.value)}
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                        </Form>

                        <Button
                            onClick={() => connectToTeams()}
                            data-testid="connect-to-teams-button"
                            type="primary"
                            disabled={!webhookURL}
                        >
                            Connect
                        </Button>
                    </FormColumn>
                </Content>
            </ContentContainer>
        </Page>
    );
};
