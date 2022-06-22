import React from 'react';
import { Tooltip } from 'antd';
import styled from 'styled-components';
import { getHealthIcon } from '../../../../../shared/health/healthUtils';
import { HealthStatus, HealthStatusType } from '../../../../../../types.generated';

const StatusContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    margin-left: 8px;
`;

type Props = {
    type: HealthStatusType;
    status: HealthStatus;
    message?: string | undefined;
};

export const EntityHealthStatus = ({ type, status, message }: Props) => {
    const icon = getHealthIcon(type, status, 18);
    return (
        <StatusContainer>
            <Tooltip title={message}>{icon}</Tooltip>
        </StatusContainer>
    );
};
