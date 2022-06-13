import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Card, Divider, Form, Input, message, Switch, Typography, Row, Col, Alert } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import {
    useGetIntegrationSettingsQuery,
    useUpdateGlobalIntegrationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { SlackIntegrationSettings } from '../../../../types.generated';
import { REDESIGN_COLORS } from '../../../entity/shared/constants';
import { Message } from '../../../shared/Message';
import { PlatformIntegrationBreadcrumb } from '../PlatformIntegrationBreadcrumb';

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

const SettingValueContainer = styled.div`
    margin-top: 12px;
`;

const StyledInput = styled(Input)`
    width: 240px;
`;

const InstructionsCard = styled(Card)`
    padding: 4px;
    margin: 16px;
`;

const InstructionsTitle = styled(Typography.Text)`
    padding: 8px;
`;

const DEFAULT_SETTINGS = {
    enabled: false,
    defaultChannelName: undefined,
    botToken: undefined,
};

export const SlackIntegration = () => {
    const [settings, setSettings] = useState<SlackIntegrationSettings>(DEFAULT_SETTINGS);

    // TODO: Determine the best way to avoid this duplicate query. (Or cache)
    const { data, loading, error, refetch } = useGetIntegrationSettingsQuery();
    const [updateGlobalIntegrationSettings] = useUpdateGlobalIntegrationSettingsMutation();

    useEffect(() => {
        if (data?.globalSettings?.integrationSettings?.slackSettings) {
            setSettings(data?.globalSettings?.integrationSettings?.slackSettings);
        }
    }, [data, setSettings]);

    const updateSlackSettings = () => {
        updateGlobalIntegrationSettings({
            variables: {
                input: {
                    slackSettings: {
                        enabled: settings.enabled,
                        defaultChannelName: settings.defaultChannelName,
                        botToken: settings.botToken,
                    },
                },
            },
        })
            .then(() => {
                message.success({ content: 'Updated Slack Settings!', duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
                }
            });
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
                <Row>
                    <Col xs={12} xl={12}>
                        <Form layout="vertical">
                            <Form.Item label={<Typography.Text strong>Enabled</Typography.Text>}>
                                <Typography.Text type="secondary">Turn the integration on or off.</Typography.Text>
                                <SettingValueContainer>
                                    <Switch
                                        checked={settings.enabled}
                                        onChange={(checked) => setSettings({ ...settings, enabled: checked })}
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item required label={<Typography.Text strong>Bot Token</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Enter a Slack bot token for your workspace.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={settings.botToken || ''}
                                        placeholder="xoya-1402430190679-2634909095557-smkeDaPL3T8KafKXiR5gjPVU"
                                        onChange={(e) => setSettings({ ...settings, botToken: e.target.value })}
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                            <Form.Item label={<Typography.Text strong>Default Channel</Typography.Text>}>
                                <Typography.Text type="secondary">
                                    Set a default channel. This is where notifications will be routed by default.
                                </Typography.Text>
                                <SettingValueContainer>
                                    <StyledInput
                                        value={settings.defaultChannelName || ''}
                                        placeholder="datahub-slack-notifications"
                                        onChange={(e) =>
                                            setSettings({ ...settings, defaultChannelName: e.target.value })
                                        }
                                    />
                                </SettingValueContainer>
                            </Form.Item>
                        </Form>
                        <Button onClick={() => updateSlackSettings()}>Update</Button>
                    </Col>
                    <Col xs={12} xl={12}>
                        <InstructionsCard
                            title={
                                <>
                                    <InfoCircleOutlined style={{ color: REDESIGN_COLORS.BLUE }} />
                                    <InstructionsTitle strong>Prerequisites</InstructionsTitle>
                                </>
                            }
                        >
                            <Typography.Paragraph>
                                Enabling the Slack integration requires that you first install the DataHub app into your
                                Slack workspace and obtain a Slack access token by following{' '}
                                <a
                                    target="_blank"
                                    rel="noreferrer"
                                    href="https://docs.acryl.io/-Mhxve3SFaX4GN0xKMZB/integrations/slack-integration"
                                >
                                    these instructions
                                </a>
                                .
                            </Typography.Paragraph>
                            <Divider />
                            <Typography.Paragraph type="secondary">
                                Stay tuned! The team at Acryl is hard at work to enable one-click integration with
                                Slack.
                            </Typography.Paragraph>
                        </InstructionsCard>
                    </Col>
                </Row>
            </ContentContainer>
        </Page>
    );
};
