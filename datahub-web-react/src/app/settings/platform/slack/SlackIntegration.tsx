import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Button, Divider, Form, Input, message, Typography, Alert, Radio, Image } from 'antd';
import { useConnectionQuery, useUpsertConnectionMutation } from '../../../../graphql/connection.generated';
import {
    useGetIntegrationSettingsQuery,
    useUpdateGlobalIntegrationSettingsMutation,
} from '../../../../graphql/settings.generated';
import slackLogo from '../../../../images/slacklogo.png';
import { DataHubConnectionDetailsType, SlackIntegrationSettings } from '../../../../types.generated';
import { Message } from '../../../shared/Message';
import { PlatformIntegrationBreadcrumb } from '../PlatformIntegrationBreadcrumb';
import { decodeSlackConnection, encodeSlackConnection, redirectToSlackInstall } from './utils';
import { SlackConnection } from './types';
import {
    APP_CONFIG_SELECT_ID,
    BOT_TOKEN_SELECT_ID,
    DEFAULT_CONNECTION,
    DEFAULT_SETTINGS,
    SLACK_CONNECTION_ID,
    SLACK_CONNECTION_OPTIONS,
    SLACK_CONNECTION_URN,
    SLACK_PLATFORM_URN,
} from './constants';
import { SlackInstructions } from './SlackInstructions';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';

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

const InstructionColumn = styled.div`
    max-width: 40%;
`;

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

const PlatformLogo = styled(Image)`
    max-height: 16px;
    margin-right: 8px;
    margin-bottom: 2px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export const SlackIntegration = () => {
    const [settings, setSettings] = useState<SlackIntegrationSettings>(DEFAULT_SETTINGS);
    const [connection, setConnection] = useState<SlackConnection>(DEFAULT_CONNECTION);
    const [selectTypeValue, setSelectTypeValue] = useState<string>(APP_CONFIG_SELECT_ID);

    const { data, loading, error, refetch } = useGetIntegrationSettingsQuery({ fetchPolicy: 'cache-first' });
    const [updateGlobalIntegrationSettings] = useUpdateGlobalIntegrationSettingsMutation();
    const { data: connData } = useConnectionQuery({
        variables: {
            urn: SLACK_CONNECTION_URN,
        },
    });
    const [upsertConnection] = useUpsertConnectionMutation();

    const slackConnData =
        connData?.connection?.details?.json && decodeSlackConnection(connData.connection?.details?.json.blob as string);

    useEffect(() => {
        if (slackConnData && connection === DEFAULT_CONNECTION) {
            setConnection(slackConnData);
        }
    }, [slackConnData, connection]);

    useEffect(() => {
        if (data?.globalSettings?.integrationSettings?.slackSettings) {
            setSettings(data?.globalSettings?.integrationSettings?.slackSettings);
        }
    }, [data, setSettings]);

    const updateSlackConnection = () => {
        upsertConnection({
            variables: {
                input: {
                    id: SLACK_CONNECTION_ID,
                    platformUrn: SLACK_PLATFORM_URN,
                    type: DataHubConnectionDetailsType.Json,
                    json: {
                        blob: encodeSlackConnection(connection),
                    },
                },
            },
        })
            .then(() => {
                analytics.event({ type: EventType.SlackIntegrationSuccessEvent, configType: selectTypeValue });
                message.success({ content: 'Updated Slack Settings!', duration: 2 });
                refetch?.();
                // If we are doing app-token path, redirect.
                if (selectTypeValue === APP_CONFIG_SELECT_ID) {
                    redirectToSlackInstall();
                }
            })
            .catch((e: unknown) => {
                analytics.event({ type: EventType.SlackIntegrationErrorEvent, configType: selectTypeValue });
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to update connection settings. An unexpected error occurred.`,
                        duration: 3,
                    });
                }
            });
    };

    const updateSlackSettings = () => {
        updateGlobalIntegrationSettings({
            variables: {
                input: {
                    slackSettings: {
                        defaultChannelName: settings.defaultChannelName,
                    },
                },
            },
        })
            .then(() => updateSlackConnection())
            .catch((e: unknown) => {
                analytics.event({ type: EventType.SlackIntegrationErrorEvent, configType: selectTypeValue });
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to update Slack settings. An unexpected error occurred.`,
                        duration: 3,
                    });
                }
            });
    };

    const isConnected =
        (slackConnData?.appConfigToken && slackConnData?.appConfigRefreshToken) ||
        slackConnData?.botToken ||
        settings.botToken;

    return (
        <Page>
            <ContentContainer>
                {loading && <Message type="loading" content="Loading settings..." style={{ marginTop: '10%' }} />}
                {error && <Alert type="error" message={error?.message || `Failed to load integration settings`} />}
                <PlatformIntegrationBreadcrumb name="Slack" />
                <Typography.Title level={3}>Slack</Typography.Title>
                <Typography.Text type="secondary">Configure an integration with Slack</Typography.Text>
                <Divider />
                <Content>
                    <FormColumn>
                        <Form layout="vertical">
                            <Form.Item label={<Typography.Text strong>Token Type</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Select a token type to use to configure the integration with Slack.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <Radio.Group
                                        options={SLACK_CONNECTION_OPTIONS}
                                        onChange={(e) => setSelectTypeValue(e.target.value)}
                                        value={selectTypeValue}
                                        optionType="button"
                                        buttonStyle="solid"
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            {selectTypeValue === BOT_TOKEN_SELECT_ID && (
                                <Form.Item required label={<Typography.Text strong>Bot Token</Typography.Text>}>
                                    <Typography.Text type="secondary">
                                        Enter a Slack bot token for your workspace.
                                    </Typography.Text>
                                    <SettingValueContainer>
                                        <StyledInput
                                            value={connection.botToken || settings.botToken || ''}
                                            placeholder="xoya-1402430190679-2634909095557-smkeDaPL3T8KafKXiR5gjPVU"
                                            data-testid="bot-token-input"
                                            onChange={(e) => setConnection({ ...connection, botToken: e.target.value })}
                                        />
                                    </SettingValueContainer>
                                </Form.Item>
                            )}
                            {selectTypeValue === APP_CONFIG_SELECT_ID && (
                                <>
                                    <Form.Item
                                        required
                                        label={<Typography.Text strong>App Configuration Token</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">
                                            Enter a Slack App Configuration Token
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={connection.appConfigToken || ''}
                                                placeholder="xoya-1402430190679-2634909095557-smkeDaPL3T8KafKXiR5gjPVU"
                                                onChange={(e) =>
                                                    setConnection({
                                                        ...connection,
                                                        appConfigToken: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item
                                        required
                                        label={
                                            <Typography.Text strong>App Configuration Refresh Token</Typography.Text>
                                        }
                                    >
                                        <Typography.Text type="secondary">
                                            Enter a Slack App Configuration Refresh Token
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={connection.appConfigRefreshToken || ''}
                                                placeholder="xoya-1402430190679"
                                                onChange={(e) =>
                                                    setConnection({
                                                        ...connection,
                                                        appConfigRefreshToken: e.target.value,
                                                    })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                </>
                            )}
                            <Form.Item label={<Typography.Text strong>Default Channel</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Set a default channel. This is where notifications will be routed by default.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={settings.defaultChannelName || ''}
                                        placeholder="datahub-slack-notifications"
                                        data-testid="default-channel-input"
                                        onChange={(e) =>
                                            setSettings({ ...settings, defaultChannelName: e.target.value })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                        </Form>

                        <Button onClick={() => updateSlackSettings()} data-testid="connect-to-slack-button">
                            <PlatformLogo preview={false} src={slackLogo} alt="slack-logo" />
                            {isConnected ? 'Re-connect to Slack' : 'Connect to Slack'}
                        </Button>
                    </FormColumn>
                    <InstructionColumn>
                        <SlackInstructions />
                    </InstructionColumn>
                </Content>
            </ContentContainer>
        </Page>
    );
};
