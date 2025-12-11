/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { TestResultsList } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsList';
import { TestResultsSummary } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsSummary';

import { TestResult } from '@types';

type Props = {
    passing: Array<TestResult>;
    failing: Array<TestResult>;
};

export const TestResults = ({ passing, failing }: Props) => {
    const filteredPassing = passing.filter((testResult) => testResult.test !== null);
    const filteredFailing = failing.filter((testResult) => testResult.test !== null);
    const totalTests = filteredPassing.length + filteredFailing.length;

    return (
        <>
            <TestResultsSummary
                summary={{
                    passing: filteredPassing.length,
                    failing: filteredFailing.length,
                    total: totalTests,
                }}
            />
            {totalTests > 0 && (
                <TestResultsList title="Governance Test Results" results={[...filteredFailing, ...filteredPassing]} />
            )}
        </>
    );
};
