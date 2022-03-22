import React from 'react';
import { Tooltip } from 'antd';
import { getHealthIcon } from '../../../../../shared/health/healthUtils';
import { HealthStatus } from '../../../../../../types.generated';

type Props = {
    status: HealthStatus;
    message?: string | undefined;
};

export const EntityHealthStatus = ({ status, message }: Props) => {
    const icon = getHealthIcon(status, 18);
    return (
        <div style={{ paddingLeft: 12, paddingRight: 12, paddingTop: 4, paddingBottom: 4 }}>
            <Tooltip title={message}>{icon}</Tooltip>
        </div>
    );
};
