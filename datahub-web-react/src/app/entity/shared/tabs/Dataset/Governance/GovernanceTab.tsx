import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { TestResults } from '@app/entity/shared/tabs/Dataset/Governance/TestResults';

/**
 * Component used for rendering the Entity Governance Tab.
 */
export const GovernanceTab = () => {
    const { entityData } = useEntityData();

    const passingTests = (entityData as any)?.testResults?.passing || [];
    const maybeFailingTests = (entityData as any)?.testResults?.failing || [];

    return <TestResults passing={passingTests} failing={maybeFailingTests} />;
};
