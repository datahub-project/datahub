import React from 'react';

import styled from 'styled-components';

import { StartStopAction } from './StartStopAction';
import { Assertion, DataContract, Monitor } from '../../../../../../../../../types.generated';
import { DeleteAction } from './DeleteAction';
import { ContractAction } from './ContractAction';
import { CopyLinkAction } from './CopyLinkAction';
import { CopyUrnAction } from './CopyUrnAction';

const ActionList = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0px 20px;
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    refetch?: () => void;
};

export const Actions = ({
    assertion,
    monitor,
    contract,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    refetch,
}: Props) => {
    return (
        <ActionList>
            <StartStopAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />
            <ContractAction
                assertion={assertion}
                monitor={monitor}
                contract={contract}
                canEdit={canEditContract}
                refetch={refetch}
            />
            <CopyLinkAction assertion={assertion} />
            <CopyUrnAction assertion={assertion} />
            <DeleteAction
                assertion={assertion}
                monitor={monitor}
                canEdit={monitor ? canEditAssertion && canEditMonitor : canEditAssertion}
                refetch={refetch}
            />
        </ActionList>
    );
};
