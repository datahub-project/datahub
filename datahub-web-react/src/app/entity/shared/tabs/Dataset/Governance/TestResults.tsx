import React from 'react';

import { TestResultsList } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsList';
import { TestResultsSummary } from '@app/entity/shared/tabs/Dataset/Governance/TestResultsSummary';
import { excludeTestsByCategories } from '@app/entityV2/shared/tabs/Dataset/Governance/testUtils';

import { TestResult } from '@types';

const CATEGORIES_TO_EXCLUDE: string[] = ['Forms'];

type Props = {
    passing: Array<TestResult>;
    failing: Array<TestResult>;
};

export const TestResults = ({ passing, failing }: Props) => {
    const filteredPassing = excludeTestsByCategories(
        passing.filter((testResult) => testResult.test !== null) || [],
        CATEGORIES_TO_EXCLUDE,
    );
    const filteredFailing = excludeTestsByCategories(
        failing.filter((testResult) => testResult.test !== null) || [],
        CATEGORIES_TO_EXCLUDE,
    );
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
