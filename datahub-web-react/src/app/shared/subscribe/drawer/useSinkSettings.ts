import {
    updateGroupNotificationSettingsFunction,
    updateUserNotificationSettingsFunction,
} from '@app/settings/personal/notifications/utils';

import {
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '@graphql/settings.generated';
import {
    EmailNotificationSettingsInput,
    NotificationSinkType,
    SlackNotificationSettings,
    SlackNotificationSettingsInput,
} from '@types';

export interface UpdateSettingsInput {
    slackSettings?: SlackNotificationSettingsInput;
    emailSettings?: EmailNotificationSettingsInput;
    sinkTypes: NotificationSinkType[];
}

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
};

// Retrieve the current settings for a user or a group.
const useActorSinkSettings = ({ isPersonal, groupUrn }: Props) => {
    const {
        data: userNotificationSettings,
        refetch: refetchUserNotificationSettings,
        error: userError,
        loading: userLoading,
    } = useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const {
        data: groupNotificationSettings,
        refetch: refetchGroupNotificationSettings,
        error: groupError,
        loading: groupLoading,
    } = useGetGroupNotificationSettingsQuery({
        skip: isPersonal || !groupUrn,
        variables: { input: { groupUrn: groupUrn || '' } },
    });

    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();
    const [updateGroupNotificationSettings] = useUpdateGroupNotificationSettingsMutation();

    const baseSinkTypes = isPersonal
        ? userNotificationSettings?.getUserNotificationSettings?.sinkTypes
        : groupNotificationSettings?.getGroupNotificationSettings?.sinkTypes;

    const onUpdateUserNotificationSettings = ({ emailSettings, slackSettings, sinkTypes }: UpdateSettingsInput) => {
        updateUserNotificationSettingsFunction({
            slackSettings,
            emailSettings,
            baseSinkTypes,
            sinkTypes,
            updateUserNotificationSettings,
            refetchUserNotificationSettings,
        });
    };

    const onUpdateGroupNotificationSettings = ({ emailSettings, slackSettings, sinkTypes }: UpdateSettingsInput) => {
        updateGroupNotificationSettingsFunction({
            groupUrn: groupUrn || '',
            slackSettings,
            emailSettings,
            baseSinkTypes,
            sinkTypes,
            updateGroupNotificationSettings,
            refetchGroupNotificationSettings,
        });
    };

    const updateSinkSettings = isPersonal ? onUpdateUserNotificationSettings : onUpdateGroupNotificationSettings;

    const origEmailSettings = isPersonal
        ? userNotificationSettings?.getUserNotificationSettings?.emailSettings
        : groupNotificationSettings?.getGroupNotificationSettings?.emailSettings;

    // To remove __typename field in result, which gets rejected when updating, we must do this
    const emailSettings = origEmailSettings ? { email: origEmailSettings.email } : undefined;

    const origSlackSettings = isPersonal
        ? (userNotificationSettings?.getUserNotificationSettings?.slackSettings as SlackNotificationSettings)
        : (groupNotificationSettings?.getGroupNotificationSettings?.slackSettings as SlackNotificationSettings);

    // To remove __typename field in result, which gets rejected when updating, we must do this
    const slackSettings = origSlackSettings
        ? { userHandle: origSlackSettings.userHandle, channels: origSlackSettings.channels }
        : undefined;

    const notificationSettings = isPersonal
        ? userNotificationSettings?.getUserNotificationSettings
        : groupNotificationSettings?.getGroupNotificationSettings;

    const refetch = isPersonal ? refetchUserNotificationSettings : refetchGroupNotificationSettings;

    const loading = isPersonal ? userLoading : groupLoading;

    const error = isPersonal ? userError : groupError;

    return {
        emailSettings,
        slackSettings,
        updateSinkSettings,
        sinkTypes: baseSinkTypes,
        notificationSettings,
        refetch,
        loading,
        error,
    } as const;
};

export default useActorSinkSettings;
