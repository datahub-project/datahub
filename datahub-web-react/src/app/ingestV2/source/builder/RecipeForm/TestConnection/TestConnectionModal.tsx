import { CheckOutlined, CloseOutlined } from '@ant-design/icons';
import { Button, Divider, Modal, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import ConnectionCapabilityView from '@app/ingestV2/source/builder/RecipeForm/TestConnection/ConnectionCapabilityView';
import {
    CapabilityReport,
    SourceCapability,
    TestConnectionResult,
} from '@app/ingestV2/source/builder/RecipeForm/TestConnection/types';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';

import LoadingSvg from '@images/datahub-logo-color-loading_pendulum.svg?react';

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 50px 0 60px 0;
`;

const LoadingSubheader = styled.div`
    display: flex;
    justify-content: center;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const LoadingHeader = styled(Typography.Title)`
    display: flex;
    justify-content: center;
`;

const ResultsHeader = styled.div<{ success: boolean }>`
    align-items: center;
    color: ${(props) => (props.success ? props.theme.colors.textSuccess : props.theme.colors.textError)};
    display: flex;
    margin-bottom: 5px;
    font-size: 20px;
    font-weight: 550;

    svg {
        margin-right: 6px;
    }
`;

const ResultsSubHeader = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
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
    color: ${(props) => props.theme.colors.textSuccess};
    margin-right: 5px;
`;

const StyledClose = styled(CloseOutlined)`
    color: ${(props) => props.theme.colors.textError};
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
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { t: tc } = useTranslation('common.actions');
    const logoUrl = useGetSourceLogoUrl(sourceConfig?.name || '');

    return (
        <Modal
            open
            onCancel={hideModal}
            footer={<Button onClick={hideModal}>{tc('done')}</Button>}
            title={
                <ModalHeader style={{ margin: 0 }}>
                    <SourceIcon alt={t('testConnection.sourceLogoAlt')} src={logoUrl} />
                    {t('testConnection.modalTitle', { sourceName: sourceConfig?.displayName })}
                </ModalHeader>
            }
            width={750}
        >
            {isLoading && (
                <ResultsWrapper>
                    <LoadingHeader level={4}>{t('testConnection.loading.title')}</LoadingHeader>
                    <LoadingSubheader>{t('testConnection.loading.subheader')}</LoadingSubheader>
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
                                <StyledClose /> {t('testConnection.failed.title')}
                            </>
                        ) : (
                            <>
                                <StyledCheck /> {t('testConnection.succeeded.title')}
                            </>
                        )}
                    </ResultsHeader>
                    <ResultsSubHeader>
                        {testConnectionFailed
                            ? t('testConnection.failed.description', { sourceName: sourceConfig?.displayName })
                            : t('testConnection.succeeded.description', { sourceName: sourceConfig?.displayName })}
                    </ResultsSubHeader>
                    <Divider />
                    {testConnectionResult?.internal_failure ? (
                        <ConnectionCapabilityView
                            capability={t('testConnection.internalFailure.label')}
                            displayMessage={testConnectionResult?.internal_failure_reason || ''}
                            success={false}
                            tooltipMessage={null}
                        />
                    ) : (
                        <CapabilitiesHeader>
                            <CapabilitiesTitle>{t('testConnection.capabilities.title')}</CapabilitiesTitle>
                            <ResultsSubHeader>{t('testConnection.capabilities.description')}</ResultsSubHeader>
                        </CapabilitiesHeader>
                    )}
                    {testConnectionResult?.basic_connectivity && (
                        <ConnectionCapabilityView
                            capability={t('testConnection.basicConnectivity.label')}
                            displayMessage={testConnectionResult?.basic_connectivity?.failure_reason}
                            success={testConnectionResult?.basic_connectivity?.capable}
                            tooltipMessage={testConnectionResult?.basic_connectivity?.mitigation_message}
                            number={1}
                        />
                    )}
                    {testConnectionResult?.capability_report &&
                        Object.keys(testConnectionResult.capability_report).map((capabilityKey, index) => {
                            return (
                                <ConnectionCapabilityView
                                    capability={SourceCapability[capabilityKey] || capabilityKey}
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
