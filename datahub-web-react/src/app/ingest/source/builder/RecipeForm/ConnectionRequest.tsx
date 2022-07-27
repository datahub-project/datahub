import {
    CheckCircleOutlined,
    CheckOutlined,
    CloseCircleOutlined,
    CloseOutlined,
    QuestionCircleOutlined,
} from '@ant-design/icons';
import { Button, message, Modal, Tooltip, Typography } from 'antd';
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

const CapabilityWrapper = styled.div<{ success: boolean }>`
    align-items: center;
    background-color: ${(props) => (props.success ? green[1] : red[1])};
    border-radius: 2px;
    display: flex;
    font-size: 14px;
    margin-bottom: 15px;
    min-height: 60px;
    padding: 5px 10px;
`;

const CapabilityName = styled.span`
    font-weight: bold;
    margin: 0 10px;
    flex: 1;
`;

const CapabilityMessage = styled.span`
    font-size: 12px;
    flex: 2;
    padding-left: 4px;
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

const StyledQuestion = styled(QuestionCircleOutlined)`
    color: rgba(0, 0, 0, 0.45);
    margin-left: 4px;
`;

interface ConnectionProps {
    success: boolean;
    capability: string;
    displayMessage: string | null;
    tooltipMessage: string | null;
}

function ConnectionCapabilityView({ success, capability, displayMessage, tooltipMessage }: ConnectionProps) {
    return (
        <CapabilityWrapper success={success}>
            {success ? <CheckOutlined style={{ color: green[6] }} /> : <CloseOutlined style={{ color: red[6] }} />}
            <CapabilityName>{capability}</CapabilityName>
            <CapabilityMessage>
                {displayMessage}
                {tooltipMessage && (
                    <Tooltip overlay={tooltipMessage}>
                        <StyledQuestion />
                    </Tooltip>
                )}
            </CapabilityMessage>
        </CapabilityWrapper>
    );
}

// interface Conn

enum SourceCapability {
    PLATFORM_INSTANCE = 'Platform Instance',
    DOMAINS = 'Domains',
    DATA_PROFILING = 'Data Profiling',
    USAGE_STATS = 'Dataset Usage',
    PARTITION_SUPPORT = 'Partition Support',
    DESCRIPTIONS = 'Descriptions',
    LINEAGE_COARSE = 'Table-Level Lineage',
    LINEAGE_FINE = 'Column-level Lineage',
    OWNERSHIP = 'Extract Ownership',
    DELETION_DETECTION = 'Detect Deleted Entities',
    TAGS = 'Extract Tags',
    CONTAINERS = 'Containers',
    SCHEMA_METADATA = 'Schema Metadata',
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

const mockSuccessResult = {
    executionRequest: {
        input: {
            source: { type: 'MANUAL_TEST_CONNECTION', __typename: 'ExecutionRequestSource' },
            __typename: 'ExecutionRequestInput',
        },
        result: {
            status: 'SUCCESS',
            report: 'test',
            structuredReport: `{
                "contentType": "application/json",
                "serializedValue": {
                    "basic_connectivity": {"capable": false, "failure_reason": "Connecting to snowflake failed due to authentication issues.", "mitigation_message": "Please ensure your credentials are correct and try again."}
                },
                "type": "TEST_CONNECTION"
              }`,
        },
        urn: 'urn:li:dataHubExecutionRequest:cd03a52a-9f9b-4164-bd33-41f93a269f4a',
        __typename: 'ExecutionRequest',
    },
};

// "capability_report": {
//     "CONTAINERS": {"capable": true, "failure_reason": null, "mitigation_message": null},
//     "SCHEMA_METADATA": {"capable": true, "failure_reason": null, "mitigation_message": null},
//     "DESCRIPTIONS": {"capable": false, "failure_reason": "You need different permissions to ingest descriptions.", "mitigation_message": "Check your credentials and talk to an admin."},
//     "DATA_PROFILING": {"capable": true, "failure_reason": null, "mitigation_message": null},
//     "LINEAGE_COARSE": {"capable": true, "failure_reason": null, "mitigation_message": null}
// }

// "DESCRIPTIONS": {"capable": false, "failure_reason": "You need different permissions to ingest descriptions.", "mitigation_message": "Check your credentials and talk to an admin."},

// "basic_connectivity": {"capable": true, "failure_reason": null, "mitigation_message": null}
// "basic_connectivity": {"capable": false, "failure_reason": "Connecting to snowflake failed due to authentication issues.", "mitigation_message": "Please ensure your credentials are correct and try again."}

// "internal_failure": True,"internal_failure_reason": "datahub library doesn't have test_connection feature. You are likely running an old version."

// serializedValue: "{\"basic_connectivity\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}, \"capability_report\": {\"CONTAINERS\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}, \"SCHEMA_METADATA\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}, \"DESCRIPTIONS\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}, \"DATA_PROFILING\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}, \"LINEAGE_COARSE\": {\"capable\": true, \"failure_reason\": null, \"mitigation_message\": null}}}"

// const mockData = [
//     { capability: 'Data Profiling', success: true, failureMessage: null },
//     { capability: 'Descriptions', success: true, failureMessage: null },
//     {
//         capability: 'Detect Deleted Entities',
//         success: true,
//         failureMessage: null,
//     },
//     { capability: 'Domains', success: true, failureMessage: null },
//     {
//         capability: 'Table-Level Lineage',
//         success: false,
//         // failureMessage: null,
//         failureMessage: 'You do not have permissions to get Table-Level Lineage.',
//     },
//     { capability: 'Platform Instance', success: true, failureMessage: null },
// ];

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

function ConnectionRequest(props: Props) {
    const { recipe } = props;
    const [isLoading, setIsLoading] = useState(false);
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [pollingInterval, setPollingInterval] = useState<null | NodeJS.Timeout>(null);
    const [testConnectionResult, setTestConnectionResult] = useState<null | TestConnectionResult>(null);
    const [createTestConnectionRequest, { data: requestData }] = useCreateTestConnectionRequestMutation();
    const [getIngestionExecutionRequest, { data: resultData }] = useGetIngestionExecutionRequestLazyQuery();

    useEffect(() => {
        if (requestData && requestData.createTestConnectionRequest) {
            const interval = setInterval(
                () => {},
                1000,
                // () =>
                //     getIngestionExecutionRequest({
                //         variables: { urn: requestData.createTestConnectionRequest as string },
                //     }),
                // 1000,
            );
            setIsLoading(true);
            setIsModalVisible(true);
            setPollingInterval(interval);
        }
    }, [requestData, getIngestionExecutionRequest]);

    useEffect(() => {
        // if (resultData) {
        const testConnectionReport = JSON.parse(mockSuccessResult.executionRequest.result.structuredReport);
        const { serializedValue } = testConnectionReport;
        setTestConnectionResult(serializedValue);
        console.log(testConnectionReport);
        // const result = resultData.executionRequest?.result;
        // if (result?.status !== RUNNING) {
        //     if (result && result.structuredReport) {
        //         const testConnectionReport = JSON.parse(result.structuredReport.serializedValue);
        //         setTestConnectionResult(testConnectionReport);
        //     }
        //     if (pollingInterval) clearInterval(pollingInterval);
        //     setIsLoading(false);
        // }
        setTimeout(() => {
            if (pollingInterval) clearInterval(pollingInterval);
            setIsLoading(false);
        }, 2000);
        // }
        // }, [pollingInterval]);
    }, [resultData, pollingInterval]);

    console.log(resultData);
    console.log(RUNNING);

    useEffect(() => {
        if (!isModalVisible) {
            if (pollingInterval) {
                clearInterval(pollingInterval);
            }
            setTestConnectionResult(null);
        }
    }, [isModalVisible, pollingInterval]);

    function testConnection() {
        const recipeJson = getRecipeJson(recipe);
        if (recipeJson) {
            // createTestConnectionRequest({ variables: { input: { recipe: recipeJson } } }).catch(() => {
            //     message.error('There was an unexpected error when trying to test your connection. Please try again.');
            // });
            console.log(createTestConnectionRequest);
            const interval = setInterval(
                () => {},
                1000,
                // () =>
                //     getIngestionExecutionRequest({
                //         variables: { urn: requestData.createTestConnectionRequest as string },
                //     }),
                // 1000,
            );
            setIsLoading(true);
            setIsModalVisible(true);
            setPollingInterval(interval);
        }
    }

    // let numFailures = 0;
    // mockData.forEach((data) => {
    //     if (!data.success) numFailures += 1;
    // });
    // const areAllSuccessful = numFailures === 0;

    console.log('testConnectionResult', testConnectionResult);

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

export default ConnectionRequest;
