import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import EmailNotificationRecipientSection from '@app/shared/subscribe/drawer/section/EmailNotificationRecipientsSection';
import SlackNotificationRecipientSection from '@app/shared/subscribe/drawer/section/SlackNotificationRecipientsSection';

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
