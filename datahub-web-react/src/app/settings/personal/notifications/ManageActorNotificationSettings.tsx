import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { EmailSinkSettingsSection } from '@app/settings/personal/notifications/section/EmailSinkSettingsSection';
import { SlackSinkSettingsSection } from '@app/settings/personal/notifications/section/SlackSinkSettingsSection';
import { EMAIL_SINK, NOTIFICATION_SINKS, SLACK_SINK } from '@app/settings/platform/types';
import { isSinkEnabled } from '@app/settings/utils';
import useActorSinkSettings from '@app/shared/subscribe/drawer/useSinkSettings';
import { useAppConfig } from '@app/useAppConfig';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';
import { EmailNotificationSettingsInput, NotificationSinkType, SlackNotificationSettingsInput } from '@types';

const NotificationSettingsTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
    margin-bottom: 12px;
`;

const NotificationSettingsContainer = styled.div`
    margin-top: 12px;
    display: flex;
    flex-direction: column;
    gap: 20px;
`;

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
    groupName?: string;
    canManageNotifications: boolean;
};

/**
 * Component used for managing actor notification settings.
 */
export const ManageActorNotificationSettings = ({ isPersonal, groupUrn, groupName, canManageNotifications }: Props) => {
    const { config } = useAppConfig();
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const { emailSettings, slackSettings, updateSinkSettings, sinkTypes } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });
    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );

    // Slack is enabled if global settings have been configured AND the actor has it enabled.
    const isSlackSinkSupported =
        canManageNotifications && globallyEnabledSinks.some((sink) => sink.id === SLACK_SINK.id);
    const isSlackSinkEnabled = isSlackSinkSupported && !!sinkTypes?.includes(SLACK_SINK.type);

    // Email is enabled if the actor has it enabled - there are no global settings.
    const isEmailSinkSupported =
        canManageNotifications && globallyEnabledSinks.some((sink) => sink.id === EMAIL_SINK.id);
    const isEmailSinkEnabled = isEmailSinkSupported && !!sinkTypes?.includes(EMAIL_SINK.type);

    const handleUpdateSlackSinkSettings = (newSlackSettings?: SlackNotificationSettingsInput) => {
        updateSinkSettings({
            slackSettings: newSlackSettings || undefined,
            emailSettings: emailSettings || undefined,
            sinkTypes: sinkTypes || [],
        });
    };

    const handleUpdateEmailSinkSettings = (newEmailSettings?: EmailNotificationSettingsInput) => {
        updateSinkSettings({
            emailSettings: newEmailSettings || undefined,
            slackSettings: slackSettings || undefined,
            sinkTypes: sinkTypes || [],
        });
    };

    const handleToggleSink = (sinkType: NotificationSinkType, enabled: boolean) => {
        const baseSinks = sinkTypes?.filter((st) => st !== sinkType) || [];
        const newSinkTypes = enabled ? [...baseSinks, sinkType] : baseSinks;
        updateSinkSettings({
            sinkTypes: newSinkTypes,
            emailSettings: emailSettings || undefined,
            slackSettings: slackSettings || undefined,
        });
    };

    const pageTitle = isPersonal ? 'My Notifications' : 'Group Notifications';

    return (
        <>
            <NotificationSettingsTitle>{pageTitle}</NotificationSettingsTitle>
            <NotificationSettingsContainer>
                <EmailSinkSettingsSection
                    isPersonal={isPersonal}
                    sinkSupported={isEmailSinkSupported}
                    sinkEnabled={isEmailSinkEnabled}
                    settings={emailSettings}
                    updateSinkSetting={handleUpdateEmailSinkSettings}
                    toggleSink={(enabled: boolean) => handleToggleSink(EMAIL_SINK.type, enabled)}
                    groupName={groupName}
                />
                <SlackSinkSettingsSection
                    isPersonal={isPersonal}
                    sinkSupported={isSlackSinkSupported}
                    sinkEnabled={isSlackSinkEnabled}
                    settings={slackSettings}
                    updateSinkSetting={handleUpdateSlackSinkSettings}
                    toggleSink={(enabled: boolean) => handleToggleSink(SLACK_SINK.type, enabled)}
                    groupName={groupName}
                />
            </NotificationSettingsContainer>
        </>
    );
};
