import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Typography, notification } from 'antd';
import type { NotificationPlacement } from 'antd/es/notification';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const NotificationTitleContainer = styled.div`
    font-weight: 700;
    line-height: 18px;
    display: flex;
    flex-direction: column;
    gap: 3px;
    margin: 0px 0px 1em -32px;
    font-family: Mulish;
`;

const NotificationHeader = styled.div`
    font-size: 12px;
`;

const NotificationDescriptionContainer = styled.div`
    font-family: Mulish;
    font-size: 14px;
    font-weight: 400;
    line-height: 18px;
    letter-spacing: 0.25px;
    margin-left: -32px;
    display: flex;
    margin-right: 25px;
    justify-content: space-between;
`;

const ViewLogsContainer = styled.div`
    cursor: pointer;
`;

const notificationConfig = {
    duration: 3,
    icon: <></>,
    closeIcon: <CloseOutlinedIcon />,
    placement: 'bottomRight' as NotificationPlacement,
};

export const openErrorNotification = (actionType: string, msg: string, viewLogs = false) => {
    notification.error({
        message: (
            <NotificationTitleContainer>
                <NotificationHeader>{actionType}</NotificationHeader>
            </NotificationTitleContainer>
        ),
        description: (
            <NotificationDescriptionContainer>
                <div>{msg}</div>
                {viewLogs && <ViewLogsContainer>View Logs</ViewLogsContainer>}
            </NotificationDescriptionContainer>
        ),
        style: {
            backgroundColor: `${REDESIGN_COLORS.RED_LIGHT}`,
            borderLeft: `4px solid ${REDESIGN_COLORS.DEPRECATION_RED_LIGHT}`,
            padding: '15px 0px',
        },
        ...notificationConfig,
    });
};

export const openSuccessNotification = (msg: string, viewLogs = false) => {
    notification.success({
        message: (
            <NotificationDescriptionContainer>
                <Typography.Text strong>{msg}</Typography.Text>
                {viewLogs && <ViewLogsContainer>View Logs</ViewLogsContainer>}
            </NotificationDescriptionContainer>
        ),
        style: {
            backgroundColor: `${REDESIGN_COLORS.GREEN_LIGHT}`,
            borderLeft: `4px solid ${REDESIGN_COLORS.GREEN_NORMAL}`,
            padding: '15px 0px',
        },
        ...notificationConfig,
    });
};
