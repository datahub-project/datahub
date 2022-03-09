import { CheckOutlined, CloseOutlined, WarningOutlined } from '@ant-design/icons';
import React from 'react';
import { HealthStatus } from '../../../types.generated';

export const getHealthColor = (status: HealthStatus) => {
    switch (status) {
        case HealthStatus.Pass: {
            return 'green';
        }
        case HealthStatus.Fail: {
            return 'red';
        }
        case HealthStatus.Warn: {
            return 'yellow';
        }
        default:
            throw new Error(`Unrecognized Health Status ${status} provided`);
    }
};

export const getHealthIcon = (status: HealthStatus, fontSize: number) => {
    switch (status) {
        case HealthStatus.Pass: {
            return <CheckOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        case HealthStatus.Fail: {
            return <CloseOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        case HealthStatus.Warn: {
            return <WarningOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        default:
            throw new Error(`Unrecognized Health Status ${status} provided`);
    }
};
