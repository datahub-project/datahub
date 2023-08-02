import { message } from 'antd';
import difference from 'lodash/difference';
import { useAppConfig } from '../../../useAppConfig';
import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { NotificationSinkType } from '../../../../types.generated';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';

export const updateUserNotificationSettingsFunction = ({
    newUserHandle,
    baseSinkTypes,
    sinkTypes,
    updateUserNotificationSettings,
    refetchUserNotificationSettings,
}: {
    newUserHandle: string;
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
                    slackSettings: {
                        userHandle: newUserHandle,
                    },
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
            setTimeout(() => {
                refetchUserNotificationSettings();
            }, 3000);
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
    newGroupChannel,
    baseSinkTypes,
    sinkTypes,
    updateGroupNotificationSettings,
    refetchGroupNotificationSettings,
}: {
    groupUrn: string;
    newGroupChannel: string;
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
                    slackSettings: {
                        channels: [newGroupChannel],
                    },
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
            setTimeout(() => {
                refetchGroupNotificationSettings();
            }, 3000);
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
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        });
};

export const useSubscriptionsEnabled = () => {
    return useAppConfig().config.featureFlags.subscriptionsEnabled;
};
