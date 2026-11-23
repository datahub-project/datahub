import { EmptyState } from '@components';
import { ShieldCheck } from '@phosphor-icons/react/dist/csr/ShieldCheck';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { TestResultsList } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResultsList';
import { TestResultsSummary } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResultsSummary';

import { TestResult } from '@types';

// Fill the tab so the cards stay put and only the results table scrolls.
const ResultsLayout = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
    overflow: hidden;
`;

const EmptyStateContainer = styled.div`
    padding: 64px 20px;
`;

type Props = {
    passing: Array<TestResult>;
    failing: Array<TestResult>;
};

export const TestResults = ({ passing, failing }: Props) => {
    const { t } = useTranslation('entity.profile.tests');
    // A test result's `test` is null when the test has since been deleted.
    const filteredPassing = passing.filter((testResult) => !!testResult.test);
    const filteredFailing = failing.filter((testResult) => !!testResult.test);
    const totalTests = filteredPassing.length + filteredFailing.length;

    if (totalTests === 0) {
        return (
            <EmptyStateContainer>
                <EmptyState
                    icon={ShieldCheck}
                    title={t('summary.noTestsRun')}
                    description={t('testResults.emptyDescription')}
                />
            </EmptyStateContainer>
        );
    }

    return (
        <ResultsLayout>
            <TestResultsSummary passing={filteredPassing.length} failing={filteredFailing.length} />
            <TestResultsList results={[...filteredFailing, ...filteredPassing]} />
        </ResultsLayout>
    );
};
