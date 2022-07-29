import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { green, red } from '@ant-design/colors';
import styled from 'styled-components/macro';
import { ReactComponent as LoadingSvg } from '../../../../../images/datahub-logo-color-loading_pendulum.svg';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import {
    useCreateTestConnectionRequestMutation,
    useGetIngestionExecutionRequestLazyQuery,
} from '../../../../../graphql/ingestion.generated';
import { RUNNING, yamlToJson } from '../../utils';
import ConnectionCapabilityView from './ConnectionCapabilityView';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 50px 0 60px 0;
`;

const LoadingSubheader = styled.div`
    display: flex;
    justify-content: center;
    font-size: 12px;
`;

const LoadingHeader = styled(Typography.Title)`
    display: flex;
    justify-content: center;
`;

const ResultsHeader = styled.div`
    align-items: center;
    display: flex;
    justify-content: center;
    margin-bottom: 24px;
    font-size: 22px;

    svg {
        margin-right: 6px;
    }
`;

const StatusWrapper = styled.span<{ color: string }>`
    color: ${(props) => props.color};
    font-weight: bold;
`;

const ResultsWrapper = styled.div`
    background-color: ${ANTD_GRAY[3]};
    border-radius: 4px;
    padding: 16px 24px;
`;

enum SourceCapability {
    PLATFORM_INSTANCE = 'Platform Instance',
    DOMAINS = 'Domains',
    DATA_PROFILING = 'Data Profiling',
    USAGE_STATS = 'Usage Stats',
    PARTITION_SUPPORT = 'Partition Support',
    DESCRIPTIONS = 'Descriptions',
    LINEAGE_COARSE = 'Table-Level Lineage',
    LINEAGE_FINE = 'Column-level Lineage',
    OWNERSHIP = 'Extract Ownership',
    DELETION_DETECTION = 'Detect Deleted Entities',
    TAGS = 'Extract Tags',
    SCHEMA_METADATA = 'Schema Metadata',
    CONTAINERS = 'Asset Containers',
}

interface ConnectionCapability {
    capable: boolean;
    failure_reason: string | null;
    mitigation_message: string | null;
}

interface CapabilityReport {
    [key: string]: ConnectionCapability;
}

interface TestConnectionResult {
    internal_failure?: boolean;
    internal_failure_reason?: string;
    basic_connectivity?: ConnectionCapability;
    capability_report?: CapabilityReport;
}

const BASIC_CONNECTIVITY_SUCCESS = "You're able to connect to this source with your given recipe.";

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
}

function TestConnectionButton(props: Props) {
    const { recipe } = props;
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
                <Modal
                    visible
                    onCancel={() => setIsModalVisible(false)}
                    footer={null}
                    title="Connection Test"
                    width={750}
                >
                    {isLoading && (
                        <ResultsWrapper>
                            <LoadingHeader level={4}>Testing your connection...</LoadingHeader>
                            <LoadingSubheader>This could take a few minutes</LoadingSubheader>
                            <LoadingWrapper>
                                <LoadingSvg height={100} width={100} />
                            </LoadingWrapper>
                        </ResultsWrapper>
                    )}
                    {!isLoading && (
                        <ResultsWrapper>
                            <ResultsHeader>
                                {testConnectionFailed ? (
                                    <>
                                        <CloseCircleOutlined style={{ color: red[5] }} /> Connection:&nbsp;
                                        <StatusWrapper color={red[5]}>Failure</StatusWrapper>
                                    </>
                                ) : (
                                    <>
                                        <CheckCircleOutlined style={{ color: green[5] }} /> Connection:&nbsp;
                                        <StatusWrapper color={green[5]}>Success!</StatusWrapper>
                                    </>
                                )}
                            </ResultsHeader>
                            {testConnectionResult?.internal_failure_reason && (
                                <ConnectionCapabilityView
                                    capability="Internal Failure"
                                    displayMessage={testConnectionResult?.internal_failure_reason}
                                    success={false}
                                    tooltipMessage={null}
                                />
                            )}
                            {testConnectionResult?.basic_connectivity && (
                                <ConnectionCapabilityView
                                    capability="Basic Connectivity"
                                    displayMessage={
                                        testConnectionResult?.basic_connectivity.capable
                                            ? BASIC_CONNECTIVITY_SUCCESS
                                            : testConnectionResult?.basic_connectivity.failure_reason
                                    }
                                    success={testConnectionResult?.basic_connectivity.capable}
                                    tooltipMessage={testConnectionResult?.basic_connectivity.mitigation_message}
                                />
                            )}
                            {testConnectionResult?.capability_report &&
                                Object.keys(testConnectionResult.capability_report).map((capabilityKey) => {
                                    return (
                                        <ConnectionCapabilityView
                                            capability={SourceCapability[capabilityKey] || ''}
                                            displayMessage={
                                                (testConnectionResult.capability_report as CapabilityReport)[
                                                    capabilityKey
                                                ].failure_reason
                                            }
                                            success={
                                                (testConnectionResult.capability_report as CapabilityReport)[
                                                    capabilityKey
                                                ].capable
                                            }
                                            tooltipMessage={
                                                (testConnectionResult.capability_report as CapabilityReport)[
                                                    capabilityKey
                                                ].mitigation_message
                                            }
                                        />
                                    );
                                })}
                        </ResultsWrapper>
                    )}
                </Modal>
            )}
        </>
    );
}

export default TestConnectionButton;
