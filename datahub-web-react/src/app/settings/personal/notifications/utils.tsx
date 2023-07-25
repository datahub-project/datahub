import { message } from 'antd';
import { useAppConfig } from '../../../useAppConfig';
import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';

export const updateUserNotificationSettingsFunction = ({
    newUserHandle,
    updateUserNotificationSettings,
    refetchUserNotificationSettings,
}: {
    newUserHandle: string;
    updateUserNotificationSettings: ReturnType<typeof useUpdateUserNotificationSettingsMutation>[0];
    refetchUserNotificationSettings: () => void;
}) => {
    updateUserNotificationSettings({
        variables: {
            input: {
                notificationSettings: {
                    slackSettings: {
                        userHandle: newUserHandle,
                    },
                },
            },
        },
    })
        .then(() => {
            refetchUserNotificationSettings();
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
    updateGroupNotificationSettings,
    refetchGroupNotificationSettings,
}: {
    groupUrn: string;
    newGroupChannel: string;
    updateGroupNotificationSettings: ReturnType<typeof useUpdateGroupNotificationSettingsMutation>[0];
    refetchGroupNotificationSettings: () => void;
}) => {
    updateGroupNotificationSettings({
        variables: {
            input: {
                groupUrn,
                notificationSettings: {
                    slackSettings: {
                        channels: [newGroupChannel],
                    },
                },
            },
        },
    })
        .then(() => {
            refetchGroupNotificationSettings();
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
