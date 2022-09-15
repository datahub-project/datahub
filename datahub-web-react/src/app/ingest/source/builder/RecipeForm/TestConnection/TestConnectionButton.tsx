import { CheckCircleOutlined } from '@ant-design/icons';
import { Button, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { green } from '@ant-design/colors';
import {
    useCreateTestConnectionRequestMutation,
    useGetIngestionExecutionRequestLazyQuery,
} from '../../../../../../graphql/ingestion.generated';
import { FAILURE, RUNNING, yamlToJson } from '../../../utils';
import { TestConnectionResult } from './types';
import TestConnectionModal from './TestConnectionModal';
import { SourceConfig } from '../../types';

export function getRecipeJson(recipeYaml: string) {
    // Convert the recipe into it's json representation, and catch + report exceptions while we do it.
    let recipeJson;
    try {
        recipeJson = yamlToJson(recipeYaml);
    } catch (e) {
        const messageText = (e as any).parsedLine
            ? `Please fix line ${(e as any).parsedLine} in your recipe.`
            : 'Please check your recipe configuration.';
        message.warn(`Found invalid YAML. ${messageText}`);
        return null;
    }
    return recipeJson;
}

interface Props {
    recipe: string;
    sourceConfigs?: SourceConfig;
}

function TestConnectionButton(props: Props) {
    const { recipe, sourceConfigs } = props;
    const [isLoading, setIsLoading] = useState(false);
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [pollingInterval, setPollingInterval] = useState<null | NodeJS.Timeout>(null);
    const [testConnectionResult, setTestConnectionResult] = useState<null | TestConnectionResult>(null);
    const [createTestConnectionRequest, { data: requestData }] = useCreateTestConnectionRequestMutation();
    const [getIngestionExecutionRequest, { data: resultData, loading }] = useGetIngestionExecutionRequestLazyQuery();

    useEffect(() => {
        if (requestData && requestData.createTestConnectionRequest) {
            const interval = setInterval(
                () =>
                    getIngestionExecutionRequest({
                        variables: { urn: requestData.createTestConnectionRequest as string },
                    }),
                2000,
            );
            setIsLoading(true);
            setIsModalVisible(true);
            setPollingInterval(interval);
        }
    }, [requestData, getIngestionExecutionRequest]);

    useEffect(() => {
        if (!loading && resultData) {
            const result = resultData.executionRequest?.result;
            if (result && result.status !== RUNNING) {
                if (result.status === FAILURE) {
                    message.error(
                        'Something went wrong with your connection test. Please check your recipe and try again.',
                    );
                    setIsModalVisible(false);
                }
                if (result.structuredReport) {
                    const testConnectionReport = JSON.parse(result.structuredReport.serializedValue);
                    setTestConnectionResult(testConnectionReport);
                }
                if (pollingInterval) clearInterval(pollingInterval);
                setIsLoading(false);
            }
        }
    }, [resultData, pollingInterval, loading]);

    useEffect(() => {
        if (!isModalVisible && pollingInterval) {
            clearInterval(pollingInterval);
        }
    }, [isModalVisible, pollingInterval]);

    function testConnection() {
        const recipeJson = getRecipeJson(recipe);
        if (recipeJson) {
            createTestConnectionRequest({ variables: { input: { recipe: recipeJson } } })
                .then((res) =>
                    getIngestionExecutionRequest({
                        variables: { urn: res.data?.createTestConnectionRequest as string },
                    }),
                )
                .catch(() => {
                    message.error(
                        'There was an unexpected error when trying to test your connection. Please try again.',
                    );
                });

            setIsLoading(true);
            setIsModalVisible(true);
        }
    }

    const internalFailure = !!testConnectionResult?.internal_failure;
    const basicConnectivityFailure = testConnectionResult?.basic_connectivity?.capable === false;
    const testConnectionFailed = internalFailure || basicConnectivityFailure;

    return (
        <>
            <Button onClick={testConnection}>
                <CheckCircleOutlined style={{ color: green[5] }} />
                Test Connection
            </Button>
            {isModalVisible && (
                <TestConnectionModal
                    isLoading={isLoading}
                    testConnectionFailed={testConnectionFailed}
                    sourceConfig={sourceConfigs}
                    testConnectionResult={testConnectionResult}
                    hideModal={() => setIsModalVisible(false)}
                />
            )}
        </>
    );
}

export default TestConnectionButton;
