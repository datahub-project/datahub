import React from 'react';
import styled from 'styled-components';

import { ContractAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ContractAction';
import { CopyLinkAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyLinkAction';
import { CopyUrnAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyUrnAction';
import { ExternalUrlAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ExternalUrlAction';
import { useIsOnSiblingsView } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { Assertion, DataContract } from '@types';

const ActionList = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0px 20px;
    gap: 8px;
`;

type Props = {
    assertion: Assertion;
    contract?: DataContract;
    canEditContract: boolean;
    refetch?: () => void;
};

export const Actions = ({ assertion, contract, canEditContract, refetch }: Props) => {
    const isSiblingsView = useIsOnSiblingsView();

    return (
        <ActionList>
            <ExternalUrlAction assertion={assertion} />
            {/** Currently, we do not handle adding to a contract in siblings mode, since we only load the root node's contract. */}
            {(!isSiblingsView && (
                <ContractAction assertion={assertion} contract={contract} canEdit={canEditContract} refetch={refetch} />
            )) ||
                null}
            <CopyLinkAction assertion={assertion} />
            <CopyUrnAction assertion={assertion} />
        </ActionList>
    );
};
