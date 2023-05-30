import { CheckOutlined, CloseOutlined } from '@ant-design/icons';
import { Button, Divider, Modal, Typography } from 'antd';
import React from 'react';
import { green, red } from '@ant-design/colors';
import styled from 'styled-components/macro';
import { ReactComponent as LoadingSvg } from '../../../../../../images/datahub-logo-color-loading_pendulum.svg';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';
import ConnectionCapabilityView from './ConnectionCapabilityView';
import { CapabilityReport, SourceCapability, TestConnectionResult } from './types';
import { SourceConfig } from '../../types';
import useGetSourceLogoUrl from '../../useGetSourceLogoUrl';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 50px 0 60px 0;
`;

const LoadingSubheader = styled.div`
    display: flex;
    justify-content: center;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

const LoadingHeader = styled(Typography.Title)`
    display: flex;
    justify-content: center;
`;

const ResultsHeader = styled.div<{ success: boolean }>`
    align-items: center;
    color: ${(props) => (props.success ? `${green[6]}` : `${red[5]}`)};
    display: flex;
    margin-bottom: 5px;
    font-size: 20px;
    font-weight: 550;

    svg {
        margin-right: 6px;
    }
`;

const ResultsSubHeader = styled.div`
    color: ${ANTD_GRAY[7]};
`;

const ResultsWrapper = styled.div`
    padding: 0 10px;
`;

const ModalHeader = styled.div`
    align-items: center;
    display: flex;
    padding: 10px 10px 0 10px;
    padding: 5px;
    font-size: 18px;
`;

const SourceIcon = styled.img`
    height: 22px;
    margin-right: 10px;
`;

const CapabilitiesHeader = styled.div`
    margin: -5px 0 20px 0;
`;

const CapabilitiesTitle = styled.div`
    font-size: 18px;
    font-weight: bold;
    margin-bottom: 5px;
`;

const StyledCheck = styled(CheckOutlined)`
    color: ${green[6]};
    margin-right: 5px;
`;

const StyledClose = styled(CloseOutlined)`
    color: ${red[5]};
    margin-right: 5px;
`;

interface Props {
    isLoading: boolean;
    testConnectionFailed: boolean;
    sourceConfig?: SourceConfig;
    testConnectionResult: TestConnectionResult | null;
    hideModal: () => void;
}

function TestConnectionModal({
    isLoading,
    testConnectionFailed,
    sourceConfig,
    testConnectionResult,
    hideModal,
}: Props) {
    const logoUrl = useGetSourceLogoUrl(sourceConfig?.name || '');

    return (
        <Modal
            visible
            onCancel={hideModal}
            footer={<Button onClick={hideModal}>Done</Button>}
            title={
                <ModalHeader style={{ margin: 0 }}>
                    <SourceIcon alt="source logo" src={logoUrl} />
                    {sourceConfig?.displayName} Connection Test
                </ModalHeader>
            }
            width={750}
        >
            {isLoading && (
                <ResultsWrapper>
                    <LoadingHeader level={4}>Testing your connection...</LoadingHeader>
                    <LoadingSubheader>This could take a few minutes.</LoadingSubheader>
                    <LoadingWrapper>
                        <LoadingSvg height={100} width={100} />
                    </LoadingWrapper>
                </ResultsWrapper>
            )}
            {!isLoading && (
                <ResultsWrapper>
                    <ResultsHeader success={!testConnectionFailed}>
                        {testConnectionFailed ? (
                            <>
                                <StyledClose /> Connection Failed
                            </>
                        ) : (
                            <>
                                <StyledCheck /> Connection Succeeded
                            </>
                        )}
                    </ResultsHeader>
                    <ResultsSubHeader>
                        {testConnectionFailed
                            ? `A connection was not able to be established with ${sourceConfig?.displayName}.`
                            : `A connection was successfully established with ${sourceConfig?.displayName}.`}
                    </ResultsSubHeader>
                    <Divider />
                    {testConnectionResult?.internal_failure ? (
                        <ConnectionCapabilityView
                            capability="Internal Failure"
                            displayMessage={testConnectionResult?.internal_failure_reason || ''}
                            success={false}
                            tooltipMessage={null}
                        />
                    ) : (
                        <CapabilitiesHeader>
                            <CapabilitiesTitle>Capabilities</CapabilitiesTitle>
                            <ResultsSubHeader>
                                The following connector capabilities are supported with your credentials
                            </ResultsSubHeader>
                        </CapabilitiesHeader>
                    )}
                    {testConnectionResult?.basic_connectivity && (
                        <ConnectionCapabilityView
                            capability="Basic Connectivity"
                            displayMessage={testConnectionResult?.basic_connectivity.failure_reason}
                            success={testConnectionResult?.basic_connectivity.capable}
                            tooltipMessage={testConnectionResult?.basic_connectivity.mitigation_message}
                            number={1}
                        />
                    )}
                    {testConnectionResult?.capability_report &&
                        Object.keys(testConnectionResult.capability_report).map((capabilityKey, index) => {
                            return (
                                <ConnectionCapabilityView
                                    capability={SourceCapability[capabilityKey] || ''}
                                    displayMessage={
                                        (testConnectionResult.capability_report as CapabilityReport)[capabilityKey]
                                            .failure_reason
                                    }
                                    success={
                                        (testConnectionResult.capability_report as CapabilityReport)[capabilityKey]
                                            .capable
                                    }
                                    tooltipMessage={
                                        (testConnectionResult.capability_report as CapabilityReport)[capabilityKey]
                                            .mitigation_message
                                    }
                                    number={index + 2} // Basic Connectivity is above with number 1
                                />
                            );
                        })}
                </ResultsWrapper>
            )}
        </Modal>
    );
}

export default TestConnectionModal;
