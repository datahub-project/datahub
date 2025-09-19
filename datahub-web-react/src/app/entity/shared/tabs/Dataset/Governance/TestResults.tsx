import React from 'react';

import { TestResultsList } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsList';
import { TestResultsSummary } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsSummary';
import { getFilteredTestResults } from '@app/entityV2/shared/tabs/Dataset/Governance/testUtils';

import { TestResult } from '@types';

type Props = {
    passing: Array<TestResult>;
    failing: Array<TestResult>;
};

export const TestResults = ({ passing, failing }: Props) => {
    const { filteredPassing, filteredFailing, totalTests } = getFilteredTestResults(passing, failing);

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
