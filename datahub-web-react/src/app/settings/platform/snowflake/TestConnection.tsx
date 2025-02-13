import React, { useState } from 'react';

import { message } from 'antd';
import { CheckCircleOutlined } from '@ant-design/icons';
import { Button } from '@src/alchemy-components';
import TestConnectionModal from '../../../ingest/source/builder/RecipeForm/TestConnection/TestConnectionModal';
import { FAILURE, RUNNING, getSourceConfigs } from '../../../ingest/source/utils';
import { SourceConfig } from '../../../ingest/source/builder/types';

import {
    useCreateTestConnectionRequestMutation,
    useGetIngestionExecutionRequestQuery,
} from '../../../../graphql/ingestion.generated';

interface Props {
    configValues: {
        account_id?: string;
        name?: string;
        password?: string;
        role?: string;
        username?: string;
        warehouse?: string;
    };
}

export const TestConnection = ({ configValues }: Props) => {
    // Test States
    const [isTesting, setIsTesting] = useState(false);
    const [isTestConnectionModalVisible, setIsTestConnectionModalVisible] = useState(false);
    const [testUrn, setTestUrn] = useState<string | undefined>();
    const [testConnectionResult, setTestConnectionResult] = useState<any | undefined>();

    // Get source configs
    const sourceConfigs = getSourceConfigs(
        [{ name: 'snowflake', displayName: 'Snowflake' } as SourceConfig],
        'snowflake',
    );

    // Mutation to test connection
    const [createTestConnectionRequest] = useCreateTestConnectionRequestMutation();

    // Query for test
    const { data: resultData } = useGetIngestionExecutionRequestQuery({
        variables: {
            urn: testUrn || '',
        },
        skip: !testUrn,
    });

    // Update test status
    React.useEffect(() => {
        const result = resultData?.executionRequest?.result;
        if (result && result.status !== RUNNING) {
            if (result.status === FAILURE) {
                message.error(
                    'Something went wrong with your connection test. Please check your recipe and try again.',
                );
                setIsTestConnectionModalVisible(false);
            }
            if (result.structuredReport) {
                const testConnectionReport = JSON.parse(result.structuredReport.serializedValue);
                setTestConnectionResult(testConnectionReport);
            }
            setIsTesting(false);
        }
    }, [resultData]);

    const handleTest = () => {
        setIsTesting(true);
        setIsTestConnectionModalVisible(true);

        createTestConnectionRequest({
            variables: {
                input: {
                    recipe: JSON.stringify({
                        source: {
                            type: 'snowflake',
                            config: configValues,
                        },
                    }),
                },
            },
        })
            .then((result: any) => {
                const urn = result?.data?.createTestConnectionRequest;
                if (urn) setTestUrn(urn);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: 'An unexpected error occurred. Failed to test connection.',
                        duration: 3,
                    });
                }
            });
    };

    const internalFailure = !!testConnectionResult?.internal_failure;
    const basicConnectivityFailure = testConnectionResult?.basic_connectivity?.capable === false;
    const testConnectionFailed = internalFailure || basicConnectivityFailure;

    const isDisabled =
        !configValues.account_id ||
        !configValues.password ||
        !configValues.username ||
        !configValues.role ||
        !configValues.warehouse;

    return (
        <>
            <Button variant="outline" onClick={handleTest} disabled={isDisabled}>
                <CheckCircleOutlined />
                Test Connection
            </Button>
            {isTestConnectionModalVisible && (
                <TestConnectionModal
                    isLoading={isTesting}
                    sourceConfig={sourceConfigs}
                    testConnectionFailed={testConnectionFailed}
                    testConnectionResult={testConnectionResult}
                    hideModal={() => setIsTestConnectionModalVisible(false)}
                />
            )}
        </>
    );
};
