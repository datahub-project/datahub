import { message } from 'antd';

export const updateUserNotificationSettingsFunction = (
    newUserHandle: string,
    updateUserNotificationSettings,
    refetchUserNotificationSettings,
) => {
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

export const updateGroupNotificationSettingsFunction = (
    groupUrn: string,
    newGroupChannel: string,
    updateGroupNotificationSettings,
    refetchGroupNotificationSettings,
) => {
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
