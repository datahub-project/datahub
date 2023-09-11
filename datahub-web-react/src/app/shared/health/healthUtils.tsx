import {
    CheckOutlined,
    CloseOutlined,
    ExclamationCircleOutlined,
    ExclamationCircleTwoTone,
    WarningOutlined,
} from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { HealthStatus, HealthStatusType, Health } from '../../../types.generated';

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
    // Currently, being unhealthy is defined as having failing assertions.
    return isFailingAssertions;
};

export const getHealthSummaryIcon = (
    healths: Health[],
    type: HealthSummaryIconType = HealthSummaryIconType.FILLED,
    fontSize = 16,
) => {
    const unhealthy = isUnhealthy(healths);
    return unhealthy
        ? (type === HealthSummaryIconType.FILLED && (
              <UnhealthyIconFilled twoToneColor={HEALTH_INDICATOR_COLOR} fontSize={fontSize} />
          )) || <UnhealthyIconOutlined fontSize={fontSize} />
        : undefined;
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

export const getHealthIcon = (type: HealthStatusType, status: HealthStatus, fontSize: number) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return getAssertionsHealthIcon(status, fontSize);
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};

export const getHealthRedirectPath = (type: HealthStatusType) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return 'Validation/Assertions';
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
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};
