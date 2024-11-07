import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { AuditOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import moment from 'moment';
import WarningIcon from '@ant-design/icons/WarningFilled';
import {
    Assertion,
    EntityType,
    DataContract,
    AssertionSourceType,
    AssertionRunEvent,
    Monitor,
} from '../../../../../../types.generated';
import { InferredAssertionPopover } from './InferredAssertionPopover';
import { InferredAssertionBadge } from './InferredAssertionBadge';
import { REDESIGN_COLORS } from '../../../constants';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { isMonitorActive } from './acrylUtils';
import { isAssertionPartOfContract } from './contract/utils';
import { AssertionDescription } from './assertion/profile/summary/AssertionDescription';
import { AssertionResultDot } from './assertion/profile/shared/AssertionResultDot';
import { AssertionResultPopover } from './assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from './assertion/profile/summary/shared/resultMessageUtils';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { AssertionListItemActions } from './assertion/profile/actions/AssertionListItemActions';

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
    const smartAssertionAgeDays = assertion.inferenceDetails?.generatedAt
        ? moment().diff(moment(assertion.inferenceDetails.generatedAt), 'days')
        : undefined;
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
    monitor?: Monitor;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    refetch?: () => void;
}

export function ActionsColumn({
    assertion,
    contract,
    monitor,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    refetch,
}: ActionsColumnProps) {
    return (
        <ActionButtonContainer>
            <AssertionListItemActions
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
