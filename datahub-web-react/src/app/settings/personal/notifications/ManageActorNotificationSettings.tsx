import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { SinkSettingsSection } from './section/SinkSettingsSection';
import { useGetGlobalSettingsQuery } from '../../../../graphql/settings.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../platform/types';
import { isSinkEnabled } from '../../utils';
import useSinkSettings from '../../../shared/subscribe/drawer/useSinkSettings';

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
    const { settingsChannel, updateSinkSettings, sinkTypes } = useSinkSettings({ isPersonal, groupUrn });

    const pageTitle = isPersonal ? 'My Notifications' : 'Group Notifications';
    const slackSinkTitle = 'Slack';
    return (
        <>
            <NotificationSettingsTitle>{pageTitle}</NotificationSettingsTitle>
            <NotificationSettingsContainer>
                <SinkSettingsSection
                    isPersonal={isPersonal}
                    sinkEnabled={slackSinkEnabled}
                    sinkName={slackSinkTitle}
                    sinkSettingValue={settingsChannel}
                    updateSinkSetting={updateSinkSettings}
                    groupName={groupName}
                    sinkTypes={sinkTypes}
                />
            </NotificationSettingsContainer>
        </>
    );
};
