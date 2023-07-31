import { message } from 'antd';
import { useAppConfig } from '../../../useAppConfig';
import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { NotificationSinkType } from '../../../../types.generated';

export const updateUserNotificationSettingsFunction = ({
    newUserHandle,
    sinkTypes,
    updateUserNotificationSettings,
    refetchUserNotificationSettings,
}: {
    newUserHandle: string;
    sinkTypes: NotificationSinkType[];
    updateUserNotificationSettings: ReturnType<typeof useUpdateUserNotificationSettingsMutation>[0];
    refetchUserNotificationSettings: () => void;
}) => {
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
            setTimeout(() => {
                refetchUserNotificationSettings();
            }, 3000);
        })
        .catch((e: unknown) => {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        });
};

export const updateGroupNotificationSettingsFunction = ({
    groupUrn,
    newGroupChannel,
    sinkTypes,
    updateGroupNotificationSettings,
    refetchGroupNotificationSettings,
}: {
    groupUrn: string;
    newGroupChannel: string;
    sinkTypes: NotificationSinkType[];
    updateGroupNotificationSettings: ReturnType<typeof useUpdateGroupNotificationSettingsMutation>[0];
    refetchGroupNotificationSettings: () => void;
}) => {
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
            setTimeout(() => {
                refetchGroupNotificationSettings();
            }, 3000);
        })
        .catch((e: unknown) => {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
            }
        });
};

export const useSubscriptionsEnabled = () => {
    return useAppConfig().config.featureFlags.subscriptionsEnabled;
};
