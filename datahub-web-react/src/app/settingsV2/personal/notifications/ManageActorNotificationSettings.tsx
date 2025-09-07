import { message } from 'antd';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components/macro';

import { EMAIL_SINK, NOTIFICATION_SINKS, SLACK_SINK, TEAMS_SINK } from '@app/settingsV2/notifications/types';
import { ActorNotificationScenarioSettings } from '@app/settingsV2/personal/notifications/ActorNotificationScenarioSettings';
import { EmailSinkSettingsSection } from '@app/settingsV2/personal/notifications/section/EmailSinkSettingsSection';
import { SlackSinkSettingsSection } from '@app/settingsV2/personal/notifications/section/SlackSinkSettingsSection';
import { TeamsSinkSettingsSection } from '@app/settingsV2/personal/notifications/section/TeamsSinkSettingsSection';
import { useIsMSFTTeamsIntegrationConfigured } from '@app/settingsV2/teams/utils';
import { isSinkEnabled } from '@app/settingsV2/utils';
import useActorSinkSettings from '@app/shared/subscribe/drawer/useSinkSettings';
import { useAppConfig } from '@app/useAppConfig';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { useUserContext } from '@src/app/context/useUserContext';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';
import {
    EmailNotificationSettingsInput,
    EntityType,
    NotificationSinkType,
    SlackNotificationSettingsInput,
    TeamsNotificationSettingsInput,
} from '@types';

const SinksContainer = styled.div`
    margin: 16px 0px;
    border-radius: 8px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const NotificationSettingsContainer = styled.div`
    margin-top: 0px;
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
    const { urn } = useUserContext();
    const { config } = useAppConfig();
    const { data: globalSettings } = useGetGlobalSettingsQuery();

    // Track whether we've already shown the Teams success message
    const hasShownTeamsSuccessMessage = useRef(false);

    // Use centralized Teams platform configuration check - query once here
    const isTeamsPlatformConfigured = useIsMSFTTeamsIntegrationConfigured();
    const {
        emailSettings,
        slackSettings,
        teamsSettings,
        updateSinkSettings,
        sinkTypes,
        notificationSettings,
        loading,
        error,
        refetch,
    } = useActorSinkSettings({
        isPersonal,
        groupUrn,
    });

    // Handle OAuth callback results from URL parameters
    useEffect(() => {
        const urlParams = new URLSearchParams(window.location.search);
        const oauthResult = urlParams.get('teams_oauth');
        const errorMessage = urlParams.get('message');
        const teamsUserId = urlParams.get('teams_user_id');

        if (oauthResult === 'success' && teamsUserId && isPersonal) {
            if (!hasShownTeamsSuccessMessage.current) {
                message.success(
                    'Teams account connected successfully! You will now receive direct message notifications.',
                );
            }
            hasShownTeamsSuccessMessage.current = true;

            // Refetch settings to get the updated Teams user data from the backend
            // The integration service has already stored the complete user info including display name
            refetch().then(() => {
                // Enable Teams sink if it's not already enabled
                if (!sinkTypes?.includes(TEAMS_SINK.type)) {
                    updateSinkSettings({
                        teamsSettings: teamsSettings || undefined,
                        emailSettings: emailSettings || undefined,
                        slackSettings: slackSettings || undefined,
                        sinkTypes: [...(sinkTypes || []), TEAMS_SINK.type],
                    });
                }

                // Clean up URL parameters after settings are refreshed
                const newUrl = window.location.pathname;
                window.history.replaceState({}, document.title, newUrl);
            });
        } else if (oauthResult === 'error') {
            const displayMessage = errorMessage
                ? decodeURIComponent(errorMessage)
                : 'Teams authentication failed. Please try again.';
            message.error(displayMessage);

            // Clean up URL parameters
            const newUrl = window.location.pathname;
            window.history.replaceState({}, document.title, newUrl);
        }
    }, [isPersonal, updateSinkSettings, emailSettings, slackSettings, sinkTypes, refetch, teamsSettings]); // Run once on component mount

    // These are the sinks that are globally allowed to be enabled.
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

    // Teams is enabled if platform integration is configured (via OAuth) AND the actor has it enabled.
    // Override the global sink check for Teams to use DataHubConnection instead of GlobalSettings
    const isTeamsFeatureEnabled = config?.featureFlags?.teamsNotificationsEnabled || false;
    const isTeamsSinkSupported = canManageNotifications && isTeamsFeatureEnabled && isTeamsPlatformConfigured;
    const isTeamsSinkEnabled = isTeamsSinkSupported && !!sinkTypes?.includes(TEAMS_SINK.type);

    const handleUpdateSlackSinkSettings = (newSlackSettings?: SlackNotificationSettingsInput) => {
        updateSinkSettings({
            slackSettings: newSlackSettings || undefined,
            emailSettings: emailSettings || undefined,
            teamsSettings: teamsSettings || undefined,
            sinkTypes: sinkTypes || [],
        });
    };

    const handleUpdateEmailSinkSettings = (newEmailSettings?: EmailNotificationSettingsInput) => {
        updateSinkSettings({
            emailSettings: newEmailSettings || undefined,
            slackSettings: slackSettings || undefined,
            teamsSettings: teamsSettings || undefined,
            sinkTypes: sinkTypes || [],
        });
    };

    const handleUpdateTeamsSinkSettings = (newTeamsSettings?: TeamsNotificationSettingsInput) => {
        // Clean Teams settings by removing __typename fields from nested objects
        let cleanTeamsSettings: TeamsNotificationSettingsInput | undefined;
        if (newTeamsSettings && (newTeamsSettings.user || newTeamsSettings.channels)) {
            cleanTeamsSettings = {
                ...newTeamsSettings,
                user: newTeamsSettings.user
                    ? {
                          teamsUserId: newTeamsSettings.user.teamsUserId,
                          azureUserId: newTeamsSettings.user.azureUserId,
                          email: newTeamsSettings.user.email,
                          displayName: newTeamsSettings.user.displayName,
                          lastUpdated: newTeamsSettings.user.lastUpdated,
                      }
                    : undefined,
                channels: newTeamsSettings.channels
                    ? newTeamsSettings.channels.map((channel) => ({
                          id: channel.id,
                          name: channel.name,
                      }))
                    : undefined,
            };

            // Remove undefined fields to clean up the object
            if (!cleanTeamsSettings.user) delete cleanTeamsSettings.user;
            if (!cleanTeamsSettings.channels) delete cleanTeamsSettings.channels;
        }

        updateSinkSettings({
            teamsSettings: cleanTeamsSettings,
            emailSettings: emailSettings || undefined,
            slackSettings: slackSettings || undefined,
            sinkTypes: sinkTypes || [],
        });
    };

    const handleToggleSink = (sinkType: NotificationSinkType, enabled: boolean) => {
        const baseSinks = sinkTypes?.filter((st) => st !== sinkType) || [];
        const newSinkTypes = enabled ? [...baseSinks, sinkType] : baseSinks;

        // Filter out settings that are null or have null values to avoid backend errors
        // Also remove __typename fields that GraphQL adds but mutation inputs don't accept
        const cleanSlackSettings = slackSettings && slackSettings.userHandle ? slackSettings : undefined;
        const cleanEmailSettings = emailSettings && emailSettings.email ? emailSettings : undefined;

        // Clean Teams settings by removing __typename fields from nested objects
        let cleanTeamsSettings: TeamsNotificationSettingsInput | undefined;

        if (sinkType === TEAMS_SINK.type && !enabled) {
            // When disabling Teams sink, completely clear the Teams settings (including user connection)
            cleanTeamsSettings = {};
        } else if (teamsSettings) {
            // Keep existing Teams settings
            cleanTeamsSettings = {
                ...teamsSettings,
                user: teamsSettings.user
                    ? {
                          teamsUserId: teamsSettings.user.teamsUserId,
                          azureUserId: teamsSettings.user.azureUserId,
                          email: teamsSettings.user.email,
                          displayName: teamsSettings.user.displayName,
                          lastUpdated: teamsSettings.user.lastUpdated,
                      }
                    : undefined,
                channels: teamsSettings.channels
                    ? teamsSettings.channels.map((channel) => ({
                          id: channel.id,
                          name: channel.name,
                      }))
                    : undefined,
            };

            // Remove undefined fields to clean up the object
            if (!cleanTeamsSettings.user) delete cleanTeamsSettings.user;
            if (!cleanTeamsSettings.channels) delete cleanTeamsSettings.channels;
        }

        updateSinkSettings({
            sinkTypes: newSinkTypes,
            emailSettings: cleanEmailSettings,
            slackSettings: cleanSlackSettings,
            teamsSettings: cleanTeamsSettings,
        });
    };

    const pageTitle = isPersonal ? 'My Notifications' : 'Group Notifications';

    return (
        <>
            <PageTitle
                title={pageTitle}
                subTitle={`Customize when and where ${
                    isPersonal ? 'you receive' : 'this group receives'
                } notifications`}
            />
            <NotificationSettingsContainer>
                <SinksContainer>
                    <EmailSinkSettingsSection
                        isPersonal={isPersonal}
                        sinkSupported={isEmailSinkSupported}
                        sinkEnabled={isEmailSinkEnabled}
                        settings={emailSettings}
                        updateSinkSetting={handleUpdateEmailSinkSettings}
                        toggleSink={(enabled: boolean) => handleToggleSink(EMAIL_SINK.type, enabled)}
                        groupName={groupName}
                        actorNotificationSettings={notificationSettings || undefined}
                        refetchNotificationSettings={refetch}
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
                    {isTeamsFeatureEnabled && (
                        <TeamsSinkSettingsSection
                            isPersonal={isPersonal}
                            sinkSupported={isTeamsSinkSupported}
                            sinkEnabled={isTeamsSinkEnabled}
                            settings={teamsSettings}
                            updateSinkSetting={handleUpdateTeamsSinkSettings}
                            toggleSink={(enabled: boolean) => handleToggleSink(TEAMS_SINK.type, enabled)}
                            groupName={groupName}
                            isTeamsPlatformConfigured={isTeamsPlatformConfigured}
                        />
                    )}
                </SinksContainer>
                <ActorNotificationScenarioSettings
                    actorUrn={(isPersonal ? urn : groupUrn) as string}
                    actorType={isPersonal ? EntityType.CorpUser : EntityType.CorpGroup}
                    actorNotificationSettings={notificationSettings || undefined}
                    loading={loading}
                    error={error}
                    refetch={refetch}
                />
            </NotificationSettingsContainer>
        </>
    );
};
