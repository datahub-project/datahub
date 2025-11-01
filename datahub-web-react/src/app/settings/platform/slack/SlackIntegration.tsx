import { blue, green } from '@ant-design/colors';
import { InfoCircleFilled } from '@ant-design/icons';
import { Alert, Button, Divider, Form, Image, Input, Modal, Radio, Typography, message } from 'antd';
import { isEqual } from 'lodash';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { PlatformIntegrationBreadcrumb } from '@app/settings/platform/PlatformIntegrationBreadcrumb';
import { SlackInstructions } from '@app/settings/platform/slack/SlackInstructions';
import { SlackIntegrationHint } from '@app/settings/platform/slack/SlackIntegrationHint';
import {
    APP_CONFIG_SELECT_ID,
    BOT_TOKEN_SELECT_ID,
    DEFAULT_CONNECTION,
    DEFAULT_SETTINGS,
    SLACK_CONNECTION_ID,
    SLACK_CONNECTION_OPTIONS,
    SLACK_CONNECTION_URN,
    SLACK_PLATFORM_URN,
} from '@app/settings/platform/slack/constants';
import { SlackConnection } from '@app/settings/platform/slack/types';
import {
    decodeSlackConnection,
    encodeSlackConnection,
    redirectToSlackInstall,
    redirectToSlackRefreshInstallation,
} from '@app/settings/platform/slack/utils';
import { Message } from '@app/shared/Message';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { useAppConfig } from '@src/app/useAppConfig';

import { useConnectionQuery, useUpsertConnectionMutation } from '@graphql/connection.generated';
import {
    useGetIntegrationSettingsQuery,
    useUpdateGlobalIntegrationSettingsMutation,
} from '@graphql/settings.generated';
import { DataHubConnectionDetailsType } from '@types';

import slackLogo from '@images/slacklogo.png';

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

const StyledPasswordInput = styled(Input.Password)`
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

const InfoIcon = styled(InfoCircleFilled)`
    color: ${green[6]};
    margin-right: 8px;
`;

const GlobalNotificationsBanner = styled.div`
    background-color: ${green[0]};
    border-radius: 8px;
    border: 1px solid ${green[6]};
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
`;

const TroubleshootLabel = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[8]};
    margin-top: 16px;
    display: block;
`;

const NewInstallButton = styled(Button)`
    background-color: transparent;
    border: 0;
    box-shadow: none;
    padding-left: 0px;
    color: ${blue[4]};
`;

const OBFUSCATED_STARS = '***';

export const SlackIntegration = () => {
    const appConfig = useAppConfig();

    const isBotTokensTabVisible = appConfig.config.featureFlags?.slackBotTokensConfigEnabled;
    const [connection, setConnection] = useState<SlackConnection>(DEFAULT_CONNECTION);
    const [selectTypeValue, setSelectTypeValue] = useState<string>(APP_CONFIG_SELECT_ID);

    const { data, loading, error, refetch } = useGetIntegrationSettingsQuery({ fetchPolicy: 'cache-first' });
    const settings = data?.globalSettings?.integrationSettings?.slackSettings ?? DEFAULT_SETTINGS;
    const [updateGlobalIntegrationSettings] = useUpdateGlobalIntegrationSettingsMutation();
    const { data: connData, loading: connLoading } = useConnectionQuery({
        variables: {
            urn: SLACK_CONNECTION_URN,
        },
    });
    const [upsertConnection] = useUpsertConnectionMutation();

    const existingConnJson = connData?.connection?.details?.json;
    const slackConnData = existingConnJson && decodeSlackConnection(existingConnJson.blob as string);
    const isConnected = slackConnData?.botToken || settings.botToken;

    useEffect(() => {
        if (slackConnData && connection === DEFAULT_CONNECTION) {
            setConnection(slackConnData);
            // render bot token tab if there bot token and no app token
            if (!slackConnData.appConfigToken && slackConnData.botToken && isBotTokensTabVisible) {
                setSelectTypeValue(BOT_TOKEN_SELECT_ID);
            }
        }
    }, [slackConnData, connection, isBotTokensTabVisible]);

    const updateSlackConnection = (isUsingAppConfigTokens: boolean, onComplete?: () => void) => {
        upsertConnection({
            variables: {
                input: {
                    id: SLACK_CONNECTION_ID,
                    platformUrn: SLACK_PLATFORM_URN,
                    type: DataHubConnectionDetailsType.Json,
                    json: {
                        blob: encodeSlackConnection(connection, isUsingAppConfigTokens),
                    },
                },
            },
        })
            .then(() => {
                analytics.event({ type: EventType.SlackIntegrationSuccessEvent, configType: selectTypeValue });
                message.success({ content: 'Updated Slack Settings!', duration: 4 });
                refetch?.();
                onComplete?.();
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

    const updateSlackSettings = (isUsingAppConfigTokens: boolean, onComplete?: () => void) => {
        updateGlobalIntegrationSettings({
            variables: {
                input: {
                    slackSettings: {
                        defaultChannelName: settings.defaultChannelName || '',
                    },
                },
            },
        })
            .then(() => {
                // If the connection has changed OR we are in the app tokens tab, update it.
                if (!isEqual(connection, slackConnData) || selectTypeValue === APP_CONFIG_SELECT_ID) {
                    updateSlackConnection(isUsingAppConfigTokens, onComplete);
                }
            })
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

    const renderConnectionButton = (): JSX.Element => {
        const isOnBotTokenTab = selectTypeValue === BOT_TOKEN_SELECT_ID;
        const isOnAppConfigTab = selectTypeValue === APP_CONFIG_SELECT_ID;

        // check if the input values are obfuscated so we can disable the connect button
        // NOTE: if user has manually entered new values then this will be set to false
        let isInputValuesObfuscated = false;
        if (isOnAppConfigTab) {
            isInputValuesObfuscated =
                !!connection.appConfigToken?.includes(OBFUSCATED_STARS) ||
                !!connection.appConfigRefreshToken?.includes(OBFUSCATED_STARS);
        } else {
            isInputValuesObfuscated =
                !!connection.botToken?.includes(OBFUSCATED_STARS) ||
                !!connection.signingSecret?.includes(OBFUSCATED_STARS);
        }

        // disable slack button if there is no app & refresh token for only App config tab
        const disableSlackButton =
            (isOnAppConfigTab && (!connection.appConfigToken || !connection.appConfigRefreshToken)) ||
            isInputValuesObfuscated;

        let slackButtonName = isConnected ? 'Reconnect to Slack' : 'Connect to Slack';
        if (isOnBotTokenTab && isConnected) {
            slackButtonName = 'Update Configuration';
        }

        return (
            <>
                <Button
                    onClick={() =>
                        updateSlackSettings(isOnAppConfigTab, () => {
                            // If we are using the app-token path, redirect once settings are updated.
                            if (isOnAppConfigTab) {
                                if (isConnected) {
                                    redirectToSlackRefreshInstallation();
                                } else {
                                    redirectToSlackInstall();
                                }
                            }
                        })
                    }
                    data-testid="connect-to-slack-button"
                    disabled={disableSlackButton}
                    style={{ display: 'block' }}
                >
                    <PlatformLogo preview={false} src={slackLogo} alt="slack-logo" />
                    {slackButtonName}
                </Button>
                {/* If bot is installed and user's on app config tab, give them a way to troubleshoot/re-install */}
                {isOnAppConfigTab && isConnected && (
                    <TroubleshootLabel>
                        Having trouble?{' '}
                        <a
                            href="https://datahubproject.io/docs/managed-datahub/slack/saas-slack-troubleshoot"
                            target="_blank"
                            rel="noreferrer"
                        >
                            Read the docs
                        </a>{' '}
                        or{' '}
                        <NewInstallButton
                            onClick={() =>
                                Modal.confirm({
                                    title: `Install a new Slack App?`,
                                    content: `This will create and install a new Slack app with the currently set App Tokens.\nEnsure the tokens entered are up-to-date.`,
                                    onOk() {
                                        updateSlackSettings(true, () => redirectToSlackInstall());
                                    },
                                    onCancel() {},
                                    okText: 'Continue',
                                    maskClosable: true,
                                    closable: true,
                                })
                            }
                        >
                            create a new installation
                        </NewInstallButton>
                    </TroubleshootLabel>
                )}
            </>
        );
    };

    return (
        <Page>
            <ContentContainer>
                {loading && <Message type="loading" content="Loading settings..." style={{ marginTop: '10%' }} />}
                {error && <Alert type="error" message={error?.message || `Failed to load integration settings`} />}
                <PlatformIntegrationBreadcrumb name="Slack" />
                <Typography.Title level={3}>Slack</Typography.Title>
                <Typography.Text type="secondary">Configure an integration with Slack</Typography.Text>
                <Divider />
                {isConnected ? (
                    <GlobalNotificationsBanner>
                        <InfoIcon />
                        The Slack integration is ready! Now switch to the{' '}
                        <Link to="/settings/notifications">Platform Notifications Tab</Link> to try it out.
                    </GlobalNotificationsBanner>
                ) : (
                    !connLoading && <SlackIntegrationHint visible={!isConnected} />
                )}
                <Content>
                    <FormColumn>
                        <Form layout="vertical">
                            {isBotTokensTabVisible ? (
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
                            ) : null}
                            {selectTypeValue === BOT_TOKEN_SELECT_ID && (
                                <>
                                    <Form.Item
                                        requiredMark="optional"
                                        required
                                        label={<Typography.Text strong>Bot Token</Typography.Text>}
                                    >
                                        <Typography.Text type="secondary">
                                            Enter a Slack bot token for your workspace.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledPasswordInput
                                                value={connection.botToken || settings.botToken || ''}
                                                placeholder="xoya-1402430190679-2634909095557-smkeDaPL3T8KafKXiR5gjPVU"
                                                data-testid="bot-token-input"
                                                onChange={(e) =>
                                                    setConnection({ ...connection, botToken: e.target.value })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item label={<Typography.Text strong>Signing Secret</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            Enter the Slack bot signing secret, which you can find at{' '}
                                            <a href="https://api.slack.com/apps">api.slack.com/apps</a>.<br />
                                            This is required for interactive actions.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledPasswordInput
                                                value={connection.signingSecret || ''}
                                                placeholder="6c698655ad405e243b298b966f20a3r3"
                                                data-testid="signing-secrets-input"
                                                onChange={(e) =>
                                                    setConnection({ ...connection, signingSecret: e.target.value })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                    <Form.Item label={<Typography.Text strong>App ID</Typography.Text>}>
                                        <Typography.Text type="secondary">
                                            Enter the Slack bot App ID, which you can find at{' '}
                                            <a href="https://api.slack.com/apps">api.slack.com/apps</a>.<br />
                                            This is required for maintaining the installation.
                                        </Typography.Text>
                                        <SettingValueContainer>
                                            <StyledInput
                                                value={connection.appId || ''}
                                                placeholder="A07D8SSHMDZ"
                                                data-testid="app-id-input"
                                                onChange={(e) =>
                                                    setConnection({ ...connection, appId: e.target.value })
                                                }
                                            />
                                        </SettingValueContainer>
                                    </Form.Item>
                                </>
                            )}
                            {selectTypeValue === APP_CONFIG_SELECT_ID && (
                                <>
                                    <Form.Item
                                        requiredMark="optional"
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
                                        requiredMark="optional"
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
                        </Form>
                        {renderConnectionButton()}
                    </FormColumn>
                    <InstructionColumn style={{ display: 'none' }}>
                        <SlackInstructions />
                    </InstructionColumn>
                </Content>
            </ContentContainer>
        </Page>
    );
};
