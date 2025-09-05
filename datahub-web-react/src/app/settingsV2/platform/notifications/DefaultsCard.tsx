import { message } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { EmailDefaults } from '@app/settingsV2/platform/notifications/EmailDefaults';
import { SlackDefaults } from '@app/settingsV2/platform/notifications/SlackDefaults';
import { TeamsDefaults } from '@app/settingsV2/platform/notifications/TeamsDefaults';
import { SLACK_CONNECTION_URN } from '@app/settingsV2/platform/slack/constants';
import { decodeSlackConnection } from '@app/settingsV2/platform/slack/utils';
import { TEAMS_CONNECTION_URN, getWebhookURL } from '@app/settingsV2/platform/teams/utils';
import { isSinkEnabled } from '@app/settingsV2/utils';
import { useAppConfig } from '@app/useAppConfig';
import { EMAIL_SINK, TEAMS_SINK } from '@src/app/settings/platform/types';
import { useConnectionQuery } from '@src/graphql/connection.generated';

import { useUpdateGlobalIntegrationSettingsMutation } from '@graphql/settings.generated';
import { GlobalSettings } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    margin-bottom: 32px;
`;

type Props = {
    globalSettings?: Partial<GlobalSettings>;
    refetch: () => void;
};

export const DefaultsCard = ({ globalSettings, refetch }: Props) => {
    const { config } = useAppConfig();

    const defaultEmailAddress = globalSettings?.integrationSettings?.emailSettings?.defaultEmail;
    const defaultSlackChannel = globalSettings?.integrationSettings?.slackSettings?.defaultChannelName;
    const defaultTeamsChannel = globalSettings?.integrationSettings?.teamsSettings?.defaultChannel;
    const defaultTeamsChannelName = defaultTeamsChannel?.name;
    const defaultTeamsChannelId = defaultTeamsChannel?.id;

    const isEmailEnabled = !!isSinkEnabled(EMAIL_SINK.id, globalSettings, config);
    const isTeamsEnabled = !!isSinkEnabled(TEAMS_SINK.id, globalSettings, config);

    const [updateGlobalIntegrationSettings] = useUpdateGlobalIntegrationSettingsMutation();

    const { data: slackConnectionData } = useConnectionQuery({
        variables: {
            urn: SLACK_CONNECTION_URN,
        },
    });

    const { data: teamsConnectionData } = useConnectionQuery({
        variables: {
            urn: TEAMS_CONNECTION_URN,
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

    const existingTeamsConnJson = teamsConnectionData?.connection?.details?.json;
    const teamsWebhookUrl = useMemo(() => {
        if (existingTeamsConnJson) {
            return getWebhookURL(existingTeamsConnJson.blob);
        }
        return undefined;
    }, [existingTeamsConnJson]);

    console.log('DefaultsCard debug:', {
        isTeamsEnabled,
        teamsWebhookUrl,
        defaultTeamsChannelId,
        defaultTeamsChannelName,
        defaultTeamsChannel,
        globalSettingsForTeams: globalSettings?.integrationSettings?.teamsSettings,
        config: config?.featureFlags,
        globalSettings,
    });

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

    const onSaveTeamsChannel = async (inputValue, displayName) => {
        try {
            const res = await updateGlobalIntegrationSettings({
                variables: {
                    input: {
                        teamsSettings: {
                            defaultChannel: inputValue ? { id: inputValue, name: displayName || inputValue } : null,
                        },
                    },
                },
            });
            if ((res as { data?: { updateGlobalSettings?: boolean } }).data?.updateGlobalSettings) {
                message.success({ content: 'Updated Teams Settings!', duration: 5 });
            }
            refetch();
        } catch (e) {
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    return (
        <Container>
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
            {isTeamsEnabled && (
                <TeamsDefaults
                    isTeamsEnabled={isTeamsEnabled}
                    channel={defaultTeamsChannelId || undefined}
                    channelName={defaultTeamsChannelName || undefined}
                    onChange={onSaveTeamsChannel}
                />
            )}
        </Container>
    );
};
