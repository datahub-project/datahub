import { message } from 'antd';
import difference from 'lodash/difference';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useAppConfig } from '@app/useAppConfig';

import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '@graphql/settings.generated';
import { EmailNotificationSettingsInput, NotificationSinkType, SlackNotificationSettingsInput } from '@types';

export const updateUserNotificationSettingsFunction = ({
    emailSettings,
    slackSettings,
    baseSinkTypes,
    sinkTypes,
    updateUserNotificationSettings,
    refetchUserNotificationSettings,
}: {
    slackSettings?: SlackNotificationSettingsInput;
    emailSettings?: EmailNotificationSettingsInput;
    baseSinkTypes: NotificationSinkType[] | undefined;
    sinkTypes: NotificationSinkType[];
    updateUserNotificationSettings: ReturnType<typeof useUpdateUserNotificationSettingsMutation>[0];
    refetchUserNotificationSettings: () => void;
}) => {
    const sinkTypesAdded = difference(sinkTypes, baseSinkTypes ?? []);
    const sinkTypesRemoved = difference(baseSinkTypes, sinkTypes);

    updateUserNotificationSettings({
        variables: {
            input: {
                notificationSettings: {
                    sinkTypes,
                    slackSettings,
                    emailSettings,
                },
            },
        },
    })
        .then(() => {
            analytics.event({
                type: EventType.NotificationSettingsSuccessEvent,
                sinkTypes,
                sinkTypesAdded,
                sinkTypesRemoved,
                actorType: 'personal',
            });
            refetchUserNotificationSettings();
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.NotificationSettingsErrorEvent,
                sinkTypes,
                sinkTypesAdded,
                sinkTypesRemoved,
                actorType: 'personal',
            });
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        });
};

export const updateGroupNotificationSettingsFunction = ({
    groupUrn,
    emailSettings,
    slackSettings,
    baseSinkTypes,
    sinkTypes,
    updateGroupNotificationSettings,
    refetchGroupNotificationSettings,
}: {
    groupUrn: string;
    slackSettings?: SlackNotificationSettingsInput;
    emailSettings?: EmailNotificationSettingsInput;
    baseSinkTypes: NotificationSinkType[] | undefined;
    sinkTypes: NotificationSinkType[];
    updateGroupNotificationSettings: ReturnType<typeof useUpdateGroupNotificationSettingsMutation>[0];
    refetchGroupNotificationSettings: () => void;
}) => {
    const sinkTypesAdded = difference(sinkTypes, baseSinkTypes ?? []);
    const sinkTypesRemoved = difference(baseSinkTypes, sinkTypes);

    updateGroupNotificationSettings({
        variables: {
            input: {
                groupUrn,
                notificationSettings: {
                    sinkTypes,
                    slackSettings,
                    emailSettings,
                },
            },
        },
    })
        .then(() => {
            analytics.event({
                type: EventType.NotificationSettingsSuccessEvent,
                sinkTypes,
                sinkTypesAdded,
                sinkTypesRemoved,
                actorType: 'group',
            });
            refetchGroupNotificationSettings();
        })
        .catch((e: unknown) => {
            analytics.event({
                type: EventType.NotificationSettingsErrorEvent,
                sinkTypes,
                sinkTypesAdded,
                sinkTypesRemoved,
                actorType: 'group',
            });
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings. An unknown error occurred!`, duration: 3 });
            }
        });
};

export const useSubscriptionsEnabled = () => {
    return useAppConfig().config.featureFlags.subscriptionsEnabled;
};
