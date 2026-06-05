import { Modal, PageTitle, Pill, Text, spacing } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { X } from '@phosphor-icons/react/dist/csr/X';
import { Typography } from 'antd';
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import {
    CapabilityReport,
    SourceCapability,
    TestConnectionResult,
} from '@app/ingestV2/source/builder/RecipeForm/TestConnection/types';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';
import { ConnectionCapabilityView } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/TestConnection/ConnectionCapabilityView';

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

const ResultsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
`;

const ModalHeader = styled.div`
    align-items: center;
    display: flex;
`;

const HeaderText = styled.div`
    display: flex;
    flex-direction: column;
`;

const SourceIcon = styled.img`
    height: 22px;
    margin-right: 10px;
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
    const logoUrl = useGetSourceLogoUrl(sourceConfig?.name || '');

    const renderTitlePill = useCallback(() => {
        if (isLoading) return null;

        if (testConnectionFailed) {
            return <Pill leftIcon={X} color="red" label={t('multiStep.testConnection.failed')} />;
        }

        return <Pill leftIcon={Check} color="green" label={t('multiStep.testConnection.successful')} />;
    }, [isLoading, testConnectionFailed, t]);

    return (
        <Modal
            open
            onCancel={hideModal}
            title={
                <ModalHeader style={{ margin: 0 }}>
                    <SourceIcon alt={t('multiStep.testConnection.sourceLogoAlt')} src={logoUrl} />
                    <HeaderText>
                        {t('multiStep.testConnection.title', { displayName: sourceConfig?.displayName })}
                        <Text color="gray">{t('multiStep.testConnection.subtitle')}</Text>
                    </HeaderText>
                </ModalHeader>
            }
            titlePill={renderTitlePill()}
            width={540}
        >
            {isLoading && (
                <ResultsWrapper>
                    <LoadingHeader level={4}>{t('multiStep.testConnection.loading')}</LoadingHeader>
                    <LoadingSubheader>{t('multiStep.testConnection.loadingSubheader')}</LoadingSubheader>
                    <LoadingWrapper>
                        <LoadingSvg height={100} width={100} />
                    </LoadingWrapper>
                </ResultsWrapper>
            )}
            {!isLoading && (
                <ResultsWrapper>
                    {testConnectionResult?.internal_failure ? (
                        <ConnectionCapabilityView
                            capability={t('multiStep.testConnection.internalFailure')}
                            displayMessage={testConnectionResult?.internal_failure_reason || ''}
                            success={false}
                            tooltipMessage={null}
                        />
                    ) : (
                        <PageTitle
                            variant="sectionHeader"
                            title={t('multiStep.testConnection.capabilities')}
                            subTitle={t('multiStep.testConnection.capabilitiesSubtitle')}
                        />
                    )}
                    {testConnectionResult?.basic_connectivity && (
                        <ConnectionCapabilityView
                            capability={t('multiStep.testConnection.basicConnectivity')}
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
