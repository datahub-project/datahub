import {
    CheckCircleOutlined,
    CheckOutlined,
    CloseOutlined,
    ExclamationCircleOutlined,
    ExclamationCircleTwoTone,
    WarningFilled,
    WarningOutlined,
} from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { HealthStatus, HealthStatusType, Health } from '../../../types.generated';
import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX } from '../../entity/shared/tabs/Incident/incidentUtils';

const HEALTH_INDICATOR_COLOR = '#d48806';

const UnhealthyIconFilled = styled(ExclamationCircleTwoTone)<{ fontSize: number }>`
    && {
        font-size: ${(props) => props.fontSize}px;
    }
`;

const UnhealthyIconOutlined = styled(ExclamationCircleOutlined)<{ fontSize: number }>`
    color: ${HEALTH_INDICATOR_COLOR};
    && {
        font-size: ${(props) => props.fontSize}px;
    }
`;

export enum HealthSummaryIconType {
    OUTLINED,
    FILLED,
}

export const isUnhealthy = (healths: Health[]) => {
    const assertionHealth = healths.find((health) => health.type === HealthStatusType.Assertions);
    const isFailingAssertions = assertionHealth?.status === HealthStatus.Fail;
    const incidentHealth = healths.find((health) => health.type === HealthStatusType.Incidents);
    const hasActiveIncidents = incidentHealth?.status === HealthStatus.Fail;
    return isFailingAssertions || hasActiveIncidents;
};

export const isHealthy = (healths: Health[]) => {
    const assertionHealth = healths.filter((health) => health.type === HealthStatusType.Assertions);
    if (assertionHealth?.length > 0) {
        const isPassingAllAssertions = assertionHealth.every((assertion) => assertion?.status === HealthStatus.Pass);
        // Currently, being healthy is defined as having passing all assertions (acryl-main).
        return isPassingAllAssertions;
    }
    return false;
};

export const getHealthSummaryIcon = (
    healths: Health[],
    type: HealthSummaryIconType = HealthSummaryIconType.FILLED,
    fontSize = 16,
) => {
    const unhealthy = isUnhealthy(healths);
    const healthy = isHealthy(healths);

    if (unhealthy) {
        const iconComponent =
            type === HealthSummaryIconType.FILLED ? (
                <UnhealthyIconFilled twoToneColor={HEALTH_INDICATOR_COLOR} fontSize={fontSize} />
            ) : (
                <UnhealthyIconOutlined fontSize={fontSize} />
            );
        return iconComponent;
    }

    if (healthy) {
        return <CheckCircleOutlined style={{ color: SUCCESS_COLOR_HEX, fontSize }} />;
    }

    return undefined;
};

export const getHealthSummaryMessage = (healths: Health[]) => {
    const unhealthy = isUnhealthy(healths);
    return unhealthy ? 'This asset may be unhealthy' : 'This asset is healthy';
};

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
        case HealthStatusType.Incidents: {
            return getIncidentsHealthIcon(status, fontSize);
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};

export const getHealthRedirectPath = (type: HealthStatusType) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return 'Quality/List';
        }
        case HealthStatusType.Incidents: {
            return 'Incidents';
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};

export const getHealthTypeName = (type: HealthStatusType) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return 'Assertions';
        }
        case HealthStatusType.Incidents: {
            return 'Incidents';
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};
