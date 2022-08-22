import {
    CheckOutlined,
    CloseOutlined,
    ExceptionOutlined,
    FileDoneOutlined,
    WarningFilled,
    WarningOutlined,
} from '@ant-design/icons';
import React from 'react';
import { HealthStatus, HealthStatusType } from '../../../types.generated';
import { FAILURE_COLOR_HEX } from '../../entity/shared/tabs/Incident/incidentUtils';

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

export const getAssertionsHealthIcon = (status: HealthStatus, fontSize: number) => {
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

// acryl-main only
export const getTestsHealthIcon = (status: HealthStatus, fontSize: number) => {
    switch (status) {
        case HealthStatus.Pass: {
            return <FileDoneOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        case HealthStatus.Fail: {
            return <ExceptionOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        case HealthStatus.Warn: {
            return <ExceptionOutlined style={{ color: getHealthColor(status), fontSize }} />;
        }
        default:
            throw new Error(`Unrecognized Health Status ${status} provided`);
    }
};

// acryl-main only
export const getIncidentsHealthIcon = (status: HealthStatus, fontSize: number) => {
    switch (status) {
        case HealthStatus.Pass: {
            // No "success" logo.
            return null;
        }
        case HealthStatus.Fail: {
            return <WarningFilled style={{ color: FAILURE_COLOR_HEX, fontSize }} />;
        }
        case HealthStatus.Warn: {
            return <WarningFilled style={{ color: FAILURE_COLOR_HEX, fontSize }} />;
        }
        default:
            throw new Error(`Unrecognized Health Status ${status} provided`);
    }
};

export const getHealthIcon = (type: HealthStatusType, status: HealthStatus, fontSize: number) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return getAssertionsHealthIcon(status, fontSize);
        }
        case HealthStatusType.Tests: {
            // acryl-main only
            return getTestsHealthIcon(status, fontSize);
        }
        case HealthStatusType.Incidents: {
            // acryl-main only
            return getIncidentsHealthIcon(status, fontSize);
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};
