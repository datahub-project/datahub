import { GenericEntityProperties } from '@src/app/entity/shared/types';

import { Health, HealthStatus, HealthStatusType } from '@types';

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
