import React from 'react';
import { TestResult } from '../../../../../../types.generated';
import { TestResultsList } from './TestResultsList';
import { TestResultsSummary } from './TestResultsSummary';

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
            {filteredFailing.length > 0 && (
                <TestResultsList title="Test Results" results={[...filteredFailing, ...filteredPassing]} />
            )}
        </>
    );
};
