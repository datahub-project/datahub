import { AuditOutlined } from '@ant-design/icons';
import WarningIcon from '@ant-design/icons/WarningFilled';
import { Tooltip } from '@components';
import moment from 'moment';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entity/shared/constants';
import { AssertionPlatformAvatar } from '@app/entity/shared/tabs/Dataset/Validations/AssertionPlatformAvatar';
import { InferredAssertionBadge } from '@app/entity/shared/tabs/Dataset/Validations/InferredAssertionBadge';
import { InferredAssertionPopover } from '@app/entity/shared/tabs/Dataset/Validations/InferredAssertionPopover';
import { isMonitorActive } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { Actions } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/Actions';
import { AssertionResultDot } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/shared/AssertionResultDot';
import { AssertionResultPopover } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { AssertionDescription } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionDescription';
import { ResultStatusType } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { isAssertionPartOfContract } from '@app/entity/shared/tabs/Dataset/Validations/contract/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { extractLatestGeneratedAt } from '@src/app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';

import {
    Assertion,
    AssertionRunEvent,
    AssertionSourceType,
    DataContract,
    DataPlatform,
    EntityType,
    Monitor,
} from '@types';

const DetailsContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    &:hover {
        cursor: pointer;
    }
    font-size: 14px;
`;

const Result = styled.div`
    margin: 0px 40px 0px 48px;
    display: flex;
    align-items: center;
`;

const ActionButtonContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: center;
`;

const AssertionPlatformWrapper = styled.div`
    margin-right: 20px;
`;

const UNKNOWN_DATA_PLATFORM = 'urn:li:dataPlatform:unknown';

const DataContractLogo = styled(AuditOutlined)`
    margin-left: 8px;
    font-size: 16px;
    color: ${REDESIGN_COLORS.BLUE};
`;

const SMART_ASSERTION_STALE_IN_DAYS = 3;

interface DetailsColumnProps {
    assertion: Assertion;
    monitor?: Monitor;
    contract?: DataContract;
    lastEvaluation?: AssertionRunEvent;
    onViewAssertionDetails: () => void;
}

export function DetailsColumn({
    assertion,
    monitor,
    contract,
    lastEvaluation,
    onViewAssertionDetails,
}: DetailsColumnProps) {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    if (!assertion.info) {
        return <>No details found</>;
    }
    const disabled = (monitor && !isMonitorActive(monitor)) || false;
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);
    const assertionInfo = assertion.info;
    const isSmartAssertion = assertionInfo.source?.type === AssertionSourceType.Inferred;
    const generatedAt = extractLatestGeneratedAt(monitor);
    const smartAssertionAgeDays = generatedAt ? moment().diff(moment(generatedAt), 'days') : undefined;
    const isSmartAssertionStale =
        isSmartAssertion && smartAssertionAgeDays && smartAssertionAgeDays > SMART_ASSERTION_STALE_IN_DAYS;
    return (
        <DetailsContainer>
            <AssertionResultPopover
                assertion={assertion}
                run={lastEvaluation}
                showProfileButton
                onClickProfileButton={onViewAssertionDetails}
                placement="right"
                resultStatusType={ResultStatusType.LATEST}
            >
                <Result>
                    <AssertionResultDot run={lastEvaluation} disabled={disabled} size={18} />
                </Result>
            </AssertionResultPopover>
            <AssertionDescription assertion={assertion} monitor={monitor} />
            {isSmartAssertionStale ? (
                <Tooltip
                    title={
                        <>
                            <b>This Smart Assertion may be outdated.</b>
                            <br />
                            This is likely related to insufficient training data for this asset. Training data is
                            obtained during ingestion syncs.
                        </>
                    }
                >
                    <WarningIcon style={{ marginLeft: 16, marginRight: 4, color: '#e9a641' }} />
                </Tooltip>
            ) : null}
            {isSmartAssertion ? (
                <InferredAssertionPopover>
                    <InferredAssertionBadge />
                </InferredAssertionPopover>
            ) : null}
            {(isPartOfContract && entityData?.urn && (
                <Tooltip
                    title={
                        <>
                            Part of Data Contract{' '}
                            <Link
                                to={`${entityRegistry.getEntityUrl(
                                    EntityType.Dataset,
                                    entityData.urn,
                                )}/Quality/Data Contract`}
                                style={{ color: REDESIGN_COLORS.BLUE }}
                            >
                                view
                            </Link>
                        </>
                    }
                >
                    <Link
                        to={`${entityRegistry.getEntityUrl(EntityType.Dataset, entityData.urn)}/Quality/Data Contract`}
                    >
                        <DataContractLogo />
                    </Link>
                </Tooltip>
            )) ||
                undefined}
        </DetailsContainer>
    );
}

interface ActionsColumnProps {
    assertion: Assertion;
    platform?: DataPlatform;
    monitor?: Monitor;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    lastEvaluationUrl?: string;
    refetch?: () => void;
}

export function ActionsColumn({
    assertion,
    platform,
    contract,
    monitor,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    lastEvaluationUrl,
    refetch,
}: ActionsColumnProps) {
    return (
        <ActionButtonContainer>
            {platform && platform.urn !== UNKNOWN_DATA_PLATFORM && (
                <AssertionPlatformWrapper>
                    <AssertionPlatformAvatar
                        platform={platform}
                        externalUrl={lastEvaluationUrl || assertion?.info?.externalUrl || undefined}
                    />
                </AssertionPlatformWrapper>
            )}
            <Actions
                assertion={assertion}
                monitor={monitor}
                contract={contract}
                canEditAssertion={canEditAssertion}
                canEditMonitor={canEditMonitor}
                canEditContract={canEditContract}
                refetch={refetch}
            />
        </ActionButtonContainer>
    );
}
