import { Card, Form, Typography, message } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { EmailDefaults } from '@app/settings/platform/notifications/EmailDefaults';
import { SlackDefaults } from '@app/settings/platform/notifications/SlackDefaults';
import { SLACK_CONNECTION_URN } from '@app/settings/platform/slack/constants';
import { decodeSlackConnection } from '@app/settings/platform/slack/utils';
import { EMAIL_SINK } from '@app/settings/platform/types';
import { isSinkEnabled } from '@app/settings/utils';
import { useAppConfig } from '@app/useAppConfig';
import { useConnectionQuery } from '@src/graphql/connection.generated';

import { useUpdateGlobalIntegrationSettingsMutation } from '@graphql/settings.generated';
import { GlobalSettings } from '@types';

const CardContainer = styled(Card)`
    margin-bottom: 24px;
    color: #373d44;
    font-size: 14px;
    font-family: Manrope;
    line-height: 20px;
    word-wrap: break-word;
`;

const SettingsHeader = styled.div`
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SettingsTitle = styled(Typography.Text)`
    font-size: 18px;
    margin-bottom: 12px;
`;

const Description = styled(Typography.Text)`
    display: block;
    margin-bottom: 20px;
    font-size: 14px;
`;

type Props = {
    globalSettings?: Partial<GlobalSettings>;
    refetch: () => void;
};

export const DefaultsCard = ({ globalSettings, refetch }: Props) => {
    const { config } = useAppConfig();

    const defaultEmailAddress = globalSettings?.integrationSettings?.emailSettings?.defaultEmail;
    const defaultSlackChannel = globalSettings?.integrationSettings?.slackSettings?.defaultChannelName;

    const isEmailEnabled = !!isSinkEnabled(EMAIL_SINK.id, globalSettings, config);

    const [updateGlobalIntegrationSettings] = useUpdateGlobalIntegrationSettingsMutation();

    const { data: slackConnectionData } = useConnectionQuery({
        variables: {
            urn: SLACK_CONNECTION_URN,
        },
    });
    const existingConnJson = slackConnectionData?.connection?.details?.json;

    const slackConnData = useMemo(() => {
        let data;
        try {
            if (existingConnJson) {
                data = decodeSlackConnection(existingConnJson.blob as string);
            }
        } catch (e) {
            return data;
        }
        return data;
    }, [existingConnJson]);

    const isSlackEnabled = !!slackConnData?.botToken;

    const onSaveSlackChannel = async (inputValue) => {
        try {
            const res = await updateGlobalIntegrationSettings({
                variables: {
                    input: {
                        slackSettings: {
                            defaultChannelName: inputValue,
                        },
                    },
                },
            });
            if ((res as { data?: { updateGlobalSettings?: boolean } }).data?.updateGlobalSettings) {
                message.success({ content: 'Updated Slack Settings!', duration: 5 });
            }
            refetch();
        } catch (e) {
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const onSaveEmailAddress = async (inputValue) => {
        try {
            const res = await updateGlobalIntegrationSettings({
                variables: {
                    input: {
                        emailSettings: {
                            defaultEmail: inputValue,
                        },
                    },
                },
            });
            if ((res as { data?: { updateGlobalSettings?: boolean } }).data?.updateGlobalSettings) {
                message.success({ content: 'Updated Email Settings!', duration: 5 });
            }
            refetch();
        } catch (e) {
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    return (
        <CardContainer>
            <SettingsHeader>
                <SettingsTitle>Channels</SettingsTitle>
            </SettingsHeader>
            <Form layout="vertical">
                <Description type="secondary">The places where you will be notified by default.</Description>
                <EmailDefaults
                    isEmailEnabled={isEmailEnabled}
                    emailAddress={defaultEmailAddress || undefined}
                    onChange={onSaveEmailAddress}
                />
                <SlackDefaults
                    isSlackEnabled={isSlackEnabled}
                    channel={defaultSlackChannel || undefined}
                    onChange={onSaveSlackChannel}
                    botToken={slackConnData?.botToken || undefined}
                />
            </Form>
        </CardContainer>
    );
};
