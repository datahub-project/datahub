import React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Tooltip, Button, message } from 'antd';
import { TestResult } from '../../../../../../types.generated';
import { TestResultsList } from './TestResultsList';
import { TestResultsSummary } from './TestResultsSummary';
import TabToolbar from '../../../components/styled/TabToolbar';
import { Message } from '../../../../../shared/Message';
import { useGetDatasetTestResultsQuery, useRunTestsMutation } from '../../../../../../graphql/test.generated';

type Props = {
    urn: string;
};

export const AcrylTestResults = ({ urn }: Props) => {
    const { data, refetch } = useGetDatasetTestResultsQuery({ variables: { urn }, fetchPolicy: 'cache-first' });
    const [runTestsMutation, { loading }] = useRunTestsMutation();
    const passingTests = (data?.dataset?.testResults?.passing?.filter((testResult) => testResult.test !== null) ||
        []) as TestResult[];
    const failingTests = (data?.dataset?.testResults?.failing?.filter((testResult) => testResult.test !== null) ||
        []) as TestResult[];
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
