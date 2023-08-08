import {
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { NotificationSinkType } from '../../../../types.generated';
import {
    updateGroupNotificationSettingsFunction,
    updateUserNotificationSettingsFunction,
} from '../../../settings/personal/notifications/utils';
import { getSettingsChannel } from './utils';

export interface UpdateSettingsInput {
    text: string;
    sinkTypes: NotificationSinkType[];
}

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

    const baseSinkTypes = isPersonal
        ? userNotificationSettings?.getUserNotificationSettings?.sinkTypes
        : groupNotificationSettings?.getGroupNotificationSettings?.sinkTypes;

    const onUpdateUserNotificationSettings = ({ text, sinkTypes }: UpdateSettingsInput) => {
        updateUserNotificationSettingsFunction({
            newUserHandle: text,
            baseSinkTypes,
            sinkTypes,
            updateUserNotificationSettings,
            refetchUserNotificationSettings,
        });
    };

    const onUpdateGroupNotificationSettings = ({ text, sinkTypes }: UpdateSettingsInput) => {
        updateGroupNotificationSettingsFunction({
            groupUrn: groupUrn || '',
            newGroupChannel: text,
            baseSinkTypes,
            sinkTypes,
            updateGroupNotificationSettings,
            refetchGroupNotificationSettings,
        });
    };

    const updateSinkSettings = isPersonal ? onUpdateUserNotificationSettings : onUpdateGroupNotificationSettings;

    const settingsChannel = getSettingsChannel(isPersonal, userNotificationSettings, groupNotificationSettings);

    return { settingsChannel, updateSinkSettings, sinkTypes: baseSinkTypes } as const;
};

export default useSinkSettings;
