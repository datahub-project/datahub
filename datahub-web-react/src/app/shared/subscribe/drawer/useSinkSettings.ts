import {
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import {
    updateGroupNotificationSettingsFunction,
    updateUserNotificationSettingsFunction,
} from '../../../settings/personal/notifications/utils';
import { getUserSettingsChannel } from './utils';

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
};

const useSinkSettings = ({ isPersonal, groupUrn }: Props) => {
    const { data: userNotificationSettings, refetch: refetchUserNotificationSettings } =
        useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const { data: groupNotificationSettings, refetch: refetchGroupNotificationSettings } =
        useGetGroupNotificationSettingsQuery({
            skip: isPersonal || !groupUrn,
            variables: { input: { groupUrn: groupUrn || '' } },
        });

    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();
    const [updateGroupNotificationSettings] = useUpdateGroupNotificationSettingsMutation();

    const onUpdateUserNotificationSettings = (newUserHandle: string) => {
        updateUserNotificationSettingsFunction(
            newUserHandle,
            updateUserNotificationSettings,
            refetchUserNotificationSettings,
        );
    };

    const onUpdateGroupNotificationSettings = (newGroupChannel: string) => {
        updateGroupNotificationSettingsFunction(
            groupUrn || '',
            newGroupChannel,
            updateGroupNotificationSettings,
            refetchGroupNotificationSettings,
        );
    };

    const updateSinkSettings = isPersonal ? onUpdateUserNotificationSettings : onUpdateGroupNotificationSettings;

    const settingsChannel = getUserSettingsChannel(isPersonal, userNotificationSettings, groupNotificationSettings);

    return { settingsChannel, updateSinkSettings } as const;
};

export default useSinkSettings;
