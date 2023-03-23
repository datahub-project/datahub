import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import UserEditProfileModal from '../../../entity/user/UserEditProfileModal';
import GroupEditModal from '../../../entity/group/GroupEditModal';
import { SinkSettingsSection } from './section/SinkSettingsSection';

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
};

/**
 * Component used for managing actor notification settings.
 */
export const ManageActorNotificationSettings = ({ isPersonal }: Props) => {
    const [showEditSettings, setShowEditSettings] = useState(false);
    const pageTitle = isPersonal ? 'My Notifications' : 'Group Notifications';
    const actorDescription = isPersonal ? 'you are' : 'the group is';
    const userModalData = {
        urn: '',
        name: '',
        title: '',
        team: '',
        email: '',
        image: '',
        slack: '',
        phone: '',
    };

    const groupModalData = {
        urn: '',
        email: '',
        slack: '',
    };

    const emailSinkTitle = 'Email Notifications';
    const emailSinkDescription = (
        <>
            Receive email notifications for entities {actorDescription} subscribed to at
            <b> your.email@yourcompany.com</b>
        </>
    );

    const slackSinkTitle = 'Slack Notifications';
    const slackSinkDescription = (
        <>
            Receive Slack notifications for entities {actorDescription} subscribed to at <b>U12345678</b>
        </>
    );

    return (
        <>
            <NotificationSettingsTitle>{pageTitle}</NotificationSettingsTitle>
            <NotificationSettingsContainer>
                <SinkSettingsSection
                    sinkTitle={emailSinkTitle}
                    sinkDescription={emailSinkDescription}
                    setShowEditSettings={setShowEditSettings}
                />
                <SinkSettingsSection
                    sinkTitle={slackSinkTitle}
                    sinkDescription={slackSinkDescription}
                    setShowEditSettings={setShowEditSettings}
                />
            </NotificationSettingsContainer>
            <UserEditProfileModal
                visible={showEditSettings && isPersonal}
                onClose={() => setShowEditSettings(false)}
                onSave={() => setShowEditSettings(false)}
                editModalData={userModalData}
            />
            <GroupEditModal
                visible={showEditSettings && !isPersonal}
                onClose={() => setShowEditSettings(false)}
                onSave={() => setShowEditSettings(false)}
                editModalData={groupModalData}
            />
        </>
    );
};
