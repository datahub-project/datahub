import React from 'react';

import styled from 'styled-components';

import { Assertion, DataContract } from '../../../../../../../../../types.generated';
import { ContractAction } from './ContractAction';
import { CopyLinkAction } from './CopyLinkAction';
import { CopyUrnAction } from './CopyUrnAction';
import { ExternalUrlAction } from './ExternalUrlAction';
import { useIsOnSiblingsView } from '../../../../../../useIsSeparateSiblingsMode';

const ActionList = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0px 20px;
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
