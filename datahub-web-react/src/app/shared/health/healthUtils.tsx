import {
    CheckCircleOutlined,
    ExclamationCircleOutlined,
    ExclamationCircleTwoTone,
} from '@ant-design/icons';
import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SUCCESS_COLOR_HEX } from '@app/entity/shared/tabs/Incident/incidentUtils';
import { GenericEntityProperties } from '@src/app/entity/shared/types';

import { Health, HealthStatus, HealthStatusType } from '@types';

const HEALTH_INDICATOR_COLOR = colors.red[500];

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

export const isDeprecated = (entity: GenericEntityProperties) => {
    return entity.deprecation?.deprecated;
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
