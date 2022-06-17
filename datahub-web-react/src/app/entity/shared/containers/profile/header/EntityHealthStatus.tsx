import React from 'react';
import { Tooltip } from 'antd';
import styled from 'styled-components';
import { getHealthIcon } from '../../../../../shared/health/healthUtils';
import { HealthStatus } from '../../../../../../types.generated';

const StatusContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    margin-left: 8px;
`;

type Props = {
    status: HealthStatus;
    message?: string | undefined;
};

export const EntityHealthStatus = ({ status, message }: Props) => {
    const icon = getHealthIcon(status, 18);
    return (
        <StatusContainer>
            <Tooltip title={message}>{icon}</Tooltip>
        </StatusContainer>
    );
};
