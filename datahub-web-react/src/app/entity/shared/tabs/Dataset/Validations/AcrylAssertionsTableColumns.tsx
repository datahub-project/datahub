import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { AuditOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import {
    Assertion,
    EntityType,
    DataContract,
    DataPlatform,
    AssertionSourceType,
    AssertionRunEvent,
    Monitor,
} from '../../../../../../types.generated';
import { InferredAssertionPopover } from './InferredAssertionPopover';
import { InferredAssertionBadge } from './InferredAssertionBadge';
import { REDESIGN_COLORS } from '../../../constants';
import { AssertionPlatformAvatar } from './AssertionPlatformAvatar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { getEntityUrnForAssertion, isMonitorActive } from './acrylUtils';
import { isAssertionPartOfContract } from './contract/utils';
import { Actions } from './assertion/profile/actions/Actions';
import { AssertionDescription } from './assertion/profile/summary/AssertionDescription';
import { AssertionResultDot } from './assertion/profile/shared/AssertionResultDot';
import { AssertionResultPopover } from './assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from './assertion/profile/summary/shared/resultUtils';

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
    if (!assertion.info) {
        return <>No details found</>;
    }
    const disabled = (monitor && !isMonitorActive(monitor)) || false;
    const assertionEntityUrn = getEntityUrnForAssertion(assertion);
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);
    const assertionInfo = assertion.info;
    const isSmartAssertion = assertionInfo.source?.type === AssertionSourceType.Inferred;
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
            <AssertionDescription assertion={assertion} />
            {isSmartAssertion && (
                <InferredAssertionPopover>
                    <InferredAssertionBadge />
                </InferredAssertionPopover>
            )}
            {(isPartOfContract && assertionEntityUrn && (
                <Tooltip
                    title={
                        <>
                            Part of Data Contract{' '}
                            <Link
                                to={`${entityRegistry.getEntityUrl(
                                    EntityType.Dataset,
                                    assertionEntityUrn,
                                )}/Validation/Data Contract`}
                                style={{ color: REDESIGN_COLORS.BLUE }}
                            >
                                view
                            </Link>
                        </>
                    }
                >
                    <Link
                        to={`${entityRegistry.getEntityUrl(
                            EntityType.Dataset,
                            assertionEntityUrn,
                        )}/Validation/Data Contract`}
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
                    <AssertionPlatformAvatar platform={platform} lastEvaluationUrl={lastEvaluationUrl} />
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
