import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { AuditOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Assertion, EntityType, DataContract, AssertionRunEvent } from '../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../constants';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
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

interface DetailsColumnProps {
    assertion: Assertion;
    contract?: DataContract;
    lastEvaluation?: AssertionRunEvent;
    onViewAssertionDetails: () => void;
}

export function DetailsColumn({ assertion, contract, lastEvaluation, onViewAssertionDetails }: DetailsColumnProps) {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    if (!assertion.info) {
        return <>No details found</>;
    }
    const disabled = false;
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);
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
            <AssertionDescription assertion={assertion} options={{ hideSecondaryLabel: true, showColumnTag: true }} />
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
    contract?: DataContract;
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
    canEditContract,
    refetch,
    shouldRightAlign,
    options,
}: ActionsColumnProps) {
    return (
        <ActionButtonContainer removeRightPadding={options?.removeRightPadding}>
            <AssertionListItemActions
                assertion={assertion}
                contract={contract}
                canEditContract={canEditContract}
                refetch={refetch}
                shouldRightAlign={shouldRightAlign}
            />
        </ActionButtonContainer>
    );
}
