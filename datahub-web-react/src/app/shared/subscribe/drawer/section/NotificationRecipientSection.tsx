import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components/macro';
import EmailNotificationRecipientSection from './EmailNotificationRecipientsSection';
import SlackNotificationRecipientSection from './SlackNotificationRecipientsSection';

const NotificationRecipientContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
`;

const NotificationRecipientTitle = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

interface Props {
    isPersonal: boolean;
}

export default function NotificationRecipientSection({ isPersonal }: Props) {
    return (
        <NotificationRecipientContainer>
            <NotificationRecipientTitle>Send notifications via</NotificationRecipientTitle>
            <EmailNotificationRecipientSection isPersonal={isPersonal} />
            <SlackNotificationRecipientSection />
        </NotificationRecipientContainer>
    );
}
