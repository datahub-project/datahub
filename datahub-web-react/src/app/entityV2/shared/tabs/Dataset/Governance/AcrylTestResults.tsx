import { ReloadOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Button, message } from 'antd';
import React from 'react';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { TestResultsList } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResultsList';
import { TestResultsSummary } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResultsSummary';
import { excludeTestsByCategories } from '@app/entityV2/shared/tabs/Dataset/Governance/testUtils';
import { Message } from '@app/shared/Message';

import { useGetDatasetTestResultsQuery, useRunTestsMutation } from '@graphql/test.generated';
import { TestResult } from '@types';

const CATEGORIES_TO_EXCLUDE: string[] = ['Forms'];

type Props = {
    urn: string;
};

export const AcrylTestResults = ({ urn }: Props) => {
    const { data, refetch } = useGetDatasetTestResultsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const [runTestsMutation, { loading }] = useRunTestsMutation();
    const passingTests = excludeTestsByCategories(
        (data?.dataset?.testResults?.passing?.filter((testResult) => testResult.test !== null) || []) as TestResult[],
        CATEGORIES_TO_EXCLUDE,
    );
    const failingTests = excludeTestsByCategories(
        (data?.dataset?.testResults?.failing?.filter((testResult) => testResult.test !== null) || []) as TestResult[],
        CATEGORIES_TO_EXCLUDE,
    );
    const totalTests = failingTests.length + passingTests.length;

    const rerunTests = () => {
        runTestsMutation({ variables: { urn } })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Completed tests!`,
                        duration: 3,
                    });
                    refetch();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: `Failed to run tests! An unknown error occurred.`, duration: 3 });
            });
    };

    return (
        <>
            {loading && <Message type="loading" content="Running tests..." />}
            <TabToolbar>
                <Tooltip title="Re-evaluate all tests for this entity." placement="right">
                    <Button type="text" icon={<ReloadOutlined style={{ fontSize: 14 }} />} onClick={rerunTests}>
                        Run Tests
                    </Button>
                </Tooltip>
            </TabToolbar>
            <TestResultsSummary
                summary={{
                    passing: passingTests.length,
                    failing: failingTests.length,
                    total: totalTests,
                }}
            />
            {totalTests > 0 && <TestResultsList title="Test Results" results={[...failingTests, ...passingTests]} />}
        </>
    );
};
