import { Modal, PageTitle, Pill, spacing } from '@components';
import { Typography } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
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
    color: ${ANTD_GRAY[7]};
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
    const logoUrl = useGetSourceLogoUrl(sourceConfig?.name || '');

    const renderTitlePill = useCallback(() => {
        if (isLoading) return null;

        if (testConnectionFailed) {
            return <Pill iconSource="phosphor" leftIcon="X" color="red" label="Failed" />;
        }

        return <Pill iconSource="phosphor" leftIcon="Check" color="green" label="Successful" />;
    }, [isLoading, testConnectionFailed]);

    return (
        <Modal
            open
            onCancel={hideModal}
            title={
                <ModalHeader style={{ margin: 0 }}>
                    <SourceIcon alt="source logo" src={logoUrl} />
                    {sourceConfig?.displayName} Connection Test
                </ModalHeader>
            }
            titlePill={renderTitlePill()}
            width={452}
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
                    {testConnectionResult?.internal_failure ? (
                        <ConnectionCapabilityView
                            capability="Internal Failure"
                            displayMessage={testConnectionResult?.internal_failure_reason || ''}
                            success={false}
                            tooltipMessage={null}
                        />
                    ) : (
                        <PageTitle
                            variant="sectionHeader"
                            title="Capabilities"
                            subTitle="The following are supported with your credentials"
                        />
                    )}
                    {testConnectionResult?.basic_connectivity && (
                        <ConnectionCapabilityView
                            capability="Basic Connectivity"
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
