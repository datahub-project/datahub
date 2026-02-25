import React from 'react';
import styled from 'styled-components';

import { AssertionName } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionName';
import { AssertionListItemActions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/AssertionListItemActions';

import { Assertion, AssertionRunEvent, DataContract } from '@types';

const DetailsContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    &:hover {
        cursor: pointer;
    }
    font-size: 14px;
`;

const ActionButtonContainer = styled.div<{ removeRightPadding?: boolean }>`
    display: flex;
    align-items: center;
    margin-left: ${(props) => (props.removeRightPadding ? 'auto' : undefined)};
`;

interface DetailsColumnProps {
    assertion: Assertion;
    contract?: DataContract;
    lastEvaluation?: AssertionRunEvent;
    onViewAssertionDetails: () => void;
}

export function DetailsColumn({ assertion, contract, lastEvaluation, onViewAssertionDetails }: DetailsColumnProps) {
    if (!assertion.info) {
        return <>No details found</>;
    }

    return (
        <DetailsContainer>
            <AssertionName
                assertion={assertion}
                lastEvaluation={lastEvaluation}
                lastEvaluationUrl={lastEvaluation?.result?.externalUrl}
                platform={assertion.platform}
                contract={contract}
                onClickProfileButton={onViewAssertionDetails}
            />
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
