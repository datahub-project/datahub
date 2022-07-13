import { CheckCircleOutlined, CheckOutlined, CloseCircleOutlined, CloseOutlined } from '@ant-design/icons';
import { Button, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { green, red } from '@ant-design/colors';
import styled from 'styled-components/macro';
import { ReactComponent as LoadingSvg } from '../../../../../images/datahub-logo-color-loading_pendulum.svg';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

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
    font-weight: bold;

    svg {
        margin-right: 6px;
    }
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

// interface Props {}

function ConnectionRequest() {
    // simply mocking this on the FE for now for demo purposes
    const [isLoading, setIsLoading] = useState(false);
    const [isModalVisible, setIsModalVisible] = useState(false);

    function testConnection() {
        setIsLoading(true);
        setIsModalVisible(true);
        setTimeout(() => setIsLoading(false), 3000);
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
                                        <CheckCircleOutlined style={{ color: green[5] }} /> Success!
                                    </>
                                ) : (
                                    <>
                                        <CloseCircleOutlined style={{ color: red[5] }} /> {numFailures} Failure
                                        {numFailures !== 1 && 's'}
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
