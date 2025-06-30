import { CheckCircleOutlined } from '@ant-design/icons';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';

import TestConnectionModal from '@app/ingest/source/builder/RecipeForm/TestConnection/TestConnectionModal';
import { TestConnectionResult } from '@app/ingest/source/builder/RecipeForm/TestConnection/types';
import { SourceConfig } from '@app/ingest/source/builder/types';
import { FAILURE, RUNNING, yamlToJson } from '@app/ingest/source/utils';
import { Button } from '@src/alchemy-components';

import {
    useCreateTestConnectionRequestMutation,
    useGetIngestionExecutionRequestLazyQuery,
} from '@graphql/ingestion.generated';

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
    version?: string | null;
}

function TestConnectionButton(props: Props) {
    const { recipe, sourceConfigs, version } = props;
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
            createTestConnectionRequest({ variables: { input: { recipe: recipeJson, version } } })
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
            <Button variant="outline" type="button" onClick={testConnection}>
                <CheckCircleOutlined />
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
