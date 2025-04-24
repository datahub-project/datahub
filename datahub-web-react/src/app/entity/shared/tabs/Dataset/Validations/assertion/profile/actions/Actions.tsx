import React from 'react';
import styled from 'styled-components';

import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import { ContractAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/ContractAction';
import { CopyLinkAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyLinkAction';
import { CopyUrnAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyUrnAction';
import { DeleteAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/DeleteAction';
import { ExternalUrlAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/ExternalUrlAction';
import { RunAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/RunAction';
import { StartStopAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/StartStopAction';
import { SubscribeAction } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/actions/SubscribeAction';

import { Assertion, DataContract, Monitor } from '@types';

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
    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    return (
        <ActionList>
            <StartStopAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />
            <ExternalUrlAction assertion={assertion} />
            <RunAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />
            <SubscribeAction assertion={assertion} refetch={refetch} />
            {/** Currently, we do not handle adding to a contract in siblings mode, since we only load the root node's contract. */}
            {(isSeparateSiblingsMode && (
                <ContractAction
                    assertion={assertion}
                    monitor={monitor}
                    contract={contract}
                    canEdit={canEditContract}
                    refetch={refetch}
                />
            )) ||
                null}
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
