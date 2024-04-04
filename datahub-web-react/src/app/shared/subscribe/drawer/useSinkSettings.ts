import {
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import {
    EmailNotificationSettingsInput,
    NotificationSinkType,
    SlackNotificationSettings,
    SlackNotificationSettingsInput,
} from '../../../../types.generated';
import {
    updateGroupNotificationSettingsFunction,
    updateUserNotificationSettingsFunction,
} from '../../../settings/personal/notifications/utils';

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
    const { data: userNotificationSettings, refetch: refetchUserNotificationSettings } =
        useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const { data: groupNotificationSettings, refetch: refetchGroupNotificationSettings } =
        useGetGroupNotificationSettingsQuery({
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

    return { emailSettings, slackSettings, updateSinkSettings, sinkTypes: baseSinkTypes } as const;
};

export default useActorSinkSettings;
