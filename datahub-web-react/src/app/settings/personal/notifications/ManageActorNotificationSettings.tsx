import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { SinkSettingsSection } from './section/SinkSettingsSection';
import {
    useGetGlobalSettingsQuery,
    useGetGroupNotificationSettingsQuery,
    useGetUserNotificationSettingsQuery,
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import { updateGroupNotificationSettingsFunction, updateUserNotificationSettingsFunction } from './utils';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../platform/types';
import { isSinkEnabled } from '../../utils';

const NotificationSettingsTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 24px;
    line-height: 32px;
    font-weight: 400;
    margin-bottom: 12px;
`;

const NotificationSettingsContainer = styled.div`
    margin-top: 12px;
    display: flex;
    flex-direction: column;
    gap: 20px;
`;

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
    groupName?: string;
};

/**
 * Component used for managing actor notification settings.
 */
export const ManageActorNotificationSettings = ({ isPersonal, groupUrn, groupName }: Props) => {
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const { data: userNotificationSettings, refetch: refetchUserNotificationSettings } =
        useGetUserNotificationSettingsQuery({ skip: !isPersonal });
    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();
    const { data: groupNotificationSettings, refetch: refetchGroupNotificationSettings } =
        useGetGroupNotificationSettingsQuery({
            skip: isPersonal || !groupUrn,
            variables: { input: { groupUrn: groupUrn || '' } },
        });
    const [updateGroupNotificationSettings] = useUpdateGroupNotificationSettingsMutation();
    const userHandle = userNotificationSettings?.getUserNotificationSettings?.slackSettings?.userHandle || undefined;
    const channels = groupNotificationSettings?.getGroupNotificationSettings?.slackSettings?.channels;
    const groupChannel = channels?.length ? channels[0] : undefined;

    const onUpdateUserNotificationSettings = (newUserHandle: string) => {
        updateUserNotificationSettingsFunction({
            newUserHandle,
            updateUserNotificationSettings,
            refetchUserNotificationSettings,
        });
    };

    const onUpdateGroupNotificationSettings = (newGroupChannel: string) => {
        updateGroupNotificationSettingsFunction({
            groupUrn: groupUrn || '',
            newGroupChannel,
            updateGroupNotificationSettings,
            refetchGroupNotificationSettings,
        });
    };

    const pageTitle = isPersonal ? 'My Notifications' : 'Group Notifications';
    const slackSinkTitle = 'Slack';
    const slackSinkSettingValue = isPersonal ? userHandle : groupChannel;
    const updateSinkSetting = isPersonal ? onUpdateUserNotificationSettings : onUpdateGroupNotificationSettings;

    return (
        <>
            <NotificationSettingsTitle>{pageTitle}</NotificationSettingsTitle>
            <NotificationSettingsContainer>
                <SinkSettingsSection
                    isPersonal={isPersonal}
                    sinkEnabled={slackSinkEnabled}
                    sinkName={slackSinkTitle}
                    sinkSettingValue={slackSinkSettingValue}
                    updateSinkSetting={updateSinkSetting}
                    groupName={groupName}
                />
            </NotificationSettingsContainer>
        </>
    );
};
