import React from 'react';
import styled from 'styled-components/macro';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { useUserContext } from '@src/app/context/useUserContext';
import { SlackSinkSettingsSection } from './section/SlackSinkSettingsSection';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';
import { EMAIL_SINK, NOTIFICATION_SINKS, SLACK_SINK } from '../../notifications/types';
import { isSinkEnabled } from '../../utils';
import useActorSinkSettings from '../../../shared/subscribe/drawer/useSinkSettings';
import {
    EmailNotificationSettingsInput,
    EntityType,
    NotificationSinkType,
    SlackNotificationSettingsInput,
} from '../../../../types.generated';
import { EmailSinkSettingsSection } from './section/EmailSinkSettingsSection';
import { useAppConfig } from '../../../useAppConfig';
import { ActorNotificationScenarioSettings } from './ActorNotificationScenarioSettings';

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
    const {
        emailSettings,
        slackSettings,
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
            <PageTitle title={pageTitle} subTitle="Customize when and where you receive personal notifications" />
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
