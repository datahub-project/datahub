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
                    // todo - fill ink sinkTypes with SLACK[] if the switch was enabled
                    // todo - implement disable slack functionality without clearing the channel
                    sinkTypes: [],
                    slackSettings: {
                        userHandle: newUserHandle,
                    },
                },
            },
        },
    })
        .then(() => {
            // todo - refetch after 3s?
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
                    // todo - fill ink sinkTypes with SLACK[] if the switch was enabled
                    // todo - implement disable slack functionality without clearing the channel
                    sinkTypes: [],
                    slackSettings: {
                        channels: [newGroupChannel],
                    },
                },
            },
        },
    })
        .then(() => {
            // todo - refetch after 3s?
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
