import { CheckCircleOutlined, CheckOutlined, CloseCircleOutlined, CloseOutlined } from '@ant-design/icons';
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
import { yamlToJson } from '../../utils';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 50px 0 60px 0;
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

const mockData = [
    { capability: 'Data Profiling', success: true, failureMessage: null },
    { capability: 'Descriptions', success: true, failureMessage: null },
    {
        capability: 'Detect Deleted Entities',
        success: true,
        failureMessage: null,
    },
    { capability: 'Domains', success: true, failureMessage: null },
    {
        capability: 'Table-Level Lineage',
        success: false,
        // failureMessage: null,
        failureMessage: 'You do not have permissions to get Table-Level Lineage.',
    },
    { capability: 'Platform Instance', success: true, failureMessage: null },
];

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
    const [createTestConnectionRequest, { data: requestData }] = useCreateTestConnectionRequestMutation();
    const [getIngestionExecutionRequest, { data: resultData }] = useGetIngestionExecutionRequestLazyQuery();

    useEffect(() => {
        if (requestData && requestData.createTestConnectionRequest) {
            const interval = setInterval(
                () =>
                    getIngestionExecutionRequest({
                        variables: { urn: requestData.createTestConnectionRequest as string },
                    }),
                1000,
            );
            setIsLoading(true);
            setIsModalVisible(true);
            setPollingInterval(interval);
        }
    }, [requestData, getIngestionExecutionRequest]);

    useEffect(() => {
        if (resultData) {
            if (resultData.executionRequest?.result) {
                if (pollingInterval) clearInterval(pollingInterval);
                setIsLoading(false);
            }
        }
    }, [resultData, pollingInterval]);

    useEffect(() => {
        if (!isModalVisible && pollingInterval) {
            clearInterval(pollingInterval);
        }
    });

    function testConnection() {
        const recipeJson = getRecipeJson(recipe);
        if (recipeJson) {
            createTestConnectionRequest({ variables: { input: { recipe: recipeJson } } }).catch(() => {
                message.error('There was an unexpected error when trying to test your connection. Please try again.');
            });
        }
    }

    let numFailures = 0;
    mockData.forEach((data) => {
        if (!data.success) numFailures += 1;
    });
    const areAllSuccessful = numFailures === 0;

    return (
        <>
            <Button onClick={testConnection}>
                <CheckCircleOutlined style={{ color: green[5] }} />
                Test Connection
            </Button>
            {isModalVisible && (
                <Modal visible onCancel={() => setIsModalVisible(false)} footer={null} width={750}>
                    <Typography.Title level={3}>Connection Test</Typography.Title>
                    {isLoading && (
                        <ResultsWrapper>
                            <LoadingHeader level={4}>Testing your connection...</LoadingHeader>
                            <LoadingWrapper>
                                <LoadingSvg height={100} width={100} />
                            </LoadingWrapper>
                        </ResultsWrapper>
                    )}
                    {!isLoading && (
                        <ResultsWrapper>
                            <ResultsHeader>
                                {areAllSuccessful ? (
                                    <>
                                        <CheckCircleOutlined style={{ color: green[5] }} /> Connection:&nbsp;
                                        <StatusWrapper color={green[5]}>Success!</StatusWrapper>
                                    </>
                                ) : (
                                    <>
                                        <CloseCircleOutlined style={{ color: red[5] }} /> Connection:&nbsp;
                                        <StatusWrapper color={red[5]}>Failure</StatusWrapper>
                                    </>
                                )}
                            </ResultsHeader>
                            {mockData.map((data) => (
                                <CapabilityWrapper success={data.success}>
                                    {data.success ? (
                                        <CheckOutlined style={{ color: green[6] }} />
                                    ) : (
                                        <CloseOutlined style={{ color: red[6] }} />
                                    )}
                                    <CapabilityName>{data.capability}</CapabilityName>
                                    <CapabilityMessage>{data.failureMessage}</CapabilityMessage>
                                </CapabilityWrapper>
                            ))}
                        </ResultsWrapper>
                    )}
                </Modal>
            )}
        </>
    );
}

export default ConnectionRequest;
