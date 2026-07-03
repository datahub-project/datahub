import { CheckCircleOutlined, ExclamationCircleOutlined, ExclamationCircleTwoTone } from '@ant-design/icons';
import i18next from 'i18next';
import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@src/app/entity/shared/types';

import { Health, HealthStatus, HealthStatusType } from '@types';

const UnhealthyIconFilled = styled(ExclamationCircleTwoTone).attrs((props) => ({
    twoToneColor: props.theme.colors.iconError,
}))<{ fontSize: number }>`
    && {
        font-size: ${(props) => props.fontSize}px;
    }
`;

const UnhealthyIconOutlined = styled(ExclamationCircleOutlined)<{ fontSize: number }>`
    color: ${(props) => props.theme.colors.iconError};
    && {
        font-size: ${(props) => props.fontSize}px;
    }
`;

const HealthyIconOutlined = styled(CheckCircleOutlined)<{ fontSize: number }>`
    color: ${(props) => props.theme.colors.iconSuccess};
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
    const testsHealth = healths.find((health) => health.type === HealthStatusType.Tests);
    const hasFailingTests = testsHealth?.status === HealthStatus.Fail;
    return isFailingAssertions || hasActiveIncidents || hasFailingTests;
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
                <UnhealthyIconFilled fontSize={fontSize} />
            ) : (
                <UnhealthyIconOutlined fontSize={fontSize} />
            );
        return iconComponent;
    }

    if (healthy) {
        return <HealthyIconOutlined fontSize={fontSize} />;
    }

    return undefined;
};

export const getHealthSummaryMessage = (healths: Health[]) => {
    const unhealthy = isUnhealthy(healths);
    return unhealthy ? i18next.t('shared.health:summaryUnhealthy') : i18next.t('shared.health:summaryHealthy');
};

export const getHealthRedirectPath = (type: HealthStatusType) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return 'Quality/List';
        }
        case HealthStatusType.Incidents: {
            return 'Incidents';
        }
        case HealthStatusType.Tests: {
            return 'Governance';
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};

export const getHealthTypeName = (type: HealthStatusType) => {
    switch (type) {
        case HealthStatusType.Assertions: {
            return i18next.t('shared.health:typeAssertions');
        }
        case HealthStatusType.Incidents: {
            return i18next.t('shared.health:typeIncidents');
        }
        case HealthStatusType.Tests: {
            return i18next.t('shared.health:typeTests');
        }
        default:
            throw new Error(`Unrecognized Health Status Type ${type} provided`);
    }
};
