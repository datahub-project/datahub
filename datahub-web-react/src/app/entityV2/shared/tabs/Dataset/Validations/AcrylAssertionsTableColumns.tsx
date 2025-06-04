import { AuditOutlined } from '@ant-design/icons';
import WarningIcon from '@ant-design/icons/WarningFilled';
import { Tooltip } from '@components';
import moment from 'moment';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { InferredAssertionBadge } from '@app/entityV2/shared/tabs/Dataset/Validations/InferredAssertionBadge';
import { InferredAssertionPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/InferredAssertionPopover';
import { extractLatestGeneratedAt, isMonitorActive } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionListItemActions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/AssertionListItemActions';
import { AssertionResultDot } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/AssertionResultDot';
import { AssertionResultPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { AssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionDescription';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { isAssertionPartOfContract } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Assertion, AssertionRunEvent, AssertionSourceType, DataContract, EntityType, Monitor } from '@types';

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

const ActionButtonContainer = styled.div<{ removeRightPadding?: boolean }>`
    display: flex;
    align-items: center;
    margin-left: ${(props) => (props.removeRightPadding ? 'auto' : undefined)};
`;

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
            <AssertionDescription
                assertion={assertion}
                monitor={monitor}
                options={{ hideSecondaryLabel: true, showColumnTag: true }}
            />
            {isSmartAssertionStale ? (
                <Tooltip
                    title={
                        <>
                            <b>This Smart Assertion may be outdated.</b>
                            <br />
                            This is likely related to insufficient training data for this asset. Training data is
                            obtained on the schedule of the assertion.
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
                            >
                                View
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
    monitor?: Monitor;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    refetch?: () => void;
    shouldRightAlign?: boolean;
    options?: {
        removeRightPadding?: boolean;
    };
}

export function ActionsColumn({
    assertion,
    contract,
    monitor,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    refetch,
    shouldRightAlign,
    options,
}: ActionsColumnProps) {
    return (
        <ActionButtonContainer removeRightPadding={options?.removeRightPadding}>
            <AssertionListItemActions
                assertion={assertion}
                monitor={monitor}
                contract={contract}
                canEditAssertion={canEditAssertion}
                canEditMonitor={canEditMonitor}
                canEditContract={canEditContract}
                refetch={refetch}
                shouldRightAlign={shouldRightAlign}
            />
        </ActionButtonContainer>
    );
}
