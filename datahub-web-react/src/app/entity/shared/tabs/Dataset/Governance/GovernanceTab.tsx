/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
