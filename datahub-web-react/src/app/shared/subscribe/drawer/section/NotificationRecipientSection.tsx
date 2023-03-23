import React from 'react';
import { Input, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

const EMAIL_TOP_ROW = 1;
const EMAIL_BOTTOM_ROW = 2;
const SLACK_TOP_ROW = 4;
const SLACK_BOTTOM_ROW = 5;
const SLACK_FOOTER_ROW = 6;

const NotificationRecipientContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
`;

const NotificationRecipientTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const NotificationSwitchContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr 15fr;
    column-gap: 8px;
    row-gap: 8px;
    margin-top: 16px;
    align-items: center;
`;

const StyledSwitch = styled(Switch)<{ row: number }>`
    grid-column: 1;
    grid-row: ${({ row }) => row};
`;

const NotificationTypeText = styled(Typography.Text)<{ row: number }>`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    grid-column: 2;
    grid-row: ${({ row }) => row};
`;

const StyledInput = styled(Input)<{ row: number }>`
    grid-column: 2;
    grid-row: ${({ row }) => row};
    max-width: 300px;
`;

const InputFooterText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
    grid-column: 2;
    grid-row: ${SLACK_FOOTER_ROW};
`;

interface Props {
    isPersonal: boolean;
}

export default function NotificationRecipientSection({ isPersonal }: Props) {
    const verticalEllipsis = String.fromCharCode(8942);
    const slackFooterText = isPersonal
        ? `Find your member ID from the ${verticalEllipsis} menu in your Slack profile`
        : 'Find the channel ID at the bottom of About in the channel details';
    return (
        <>
            <NotificationRecipientContainer>
                <NotificationRecipientTitle>Send notifications via</NotificationRecipientTitle>
                <NotificationSwitchContainer>
                    <StyledSwitch row={EMAIL_TOP_ROW} size="small" />
                    <NotificationTypeText row={EMAIL_TOP_ROW}> Email Notifications </NotificationTypeText>
                    <StyledInput row={EMAIL_BOTTOM_ROW} placeholder="engineering@example.com" />
                    <StyledSwitch row={SLACK_TOP_ROW} size="small" />
                    <NotificationTypeText row={SLACK_TOP_ROW}> Slack Notifications </NotificationTypeText>
                    <StyledInput row={SLACK_BOTTOM_ROW} placeholder="U123456" />
                    <InputFooterText>{slackFooterText}</InputFooterText>
                </NotificationSwitchContainer>
            </NotificationRecipientContainer>
        </>
    );
}
