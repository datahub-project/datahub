import React from 'react';

import styled from 'styled-components';

import { StartStopAction } from './StartStopAction';
import { Assertion, DataContract, Monitor } from '../../../../../../../../../types.generated';
import { DeleteAction } from './DeleteAction';
import { ContractAction } from './ContractAction';
import { CopyLinkAction } from './CopyLinkAction';
import { CopyUrnAction } from './CopyUrnAction';
import { SubscribeAction } from './SubscribeAction';
import { RunAction } from './RunAction';
import { ExternalUrlAction } from './ExternalUrlAction';
import { useIsSeparateSiblingsMode } from '../../../../../../useIsSeparateSiblingsMode';
import { Button, Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';

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

export const AssertionActionList = ({
    assertion,
    monitor,
    contract,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    refetch,
}: Props) => {
    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();

    const menu = (
        <Menu>
            {/** Currently, we do not handle adding to a contract in siblings mode, since we only load the root node's contract. */}
            {(isSeparateSiblingsMode && (
                <Menu.Item key="1">
                    <ContractAction
                        assertion={assertion}
                        monitor={monitor}
                        contract={contract}
                        canEdit={canEditContract}
                        refetch={refetch}
                    />
                </Menu.Item>
            )) ||
                null}
            <Menu.Item key="2">
                <ExternalUrlAction assertion={assertion} />
            </Menu.Item>
            <Menu.Item key="3">
                <RunAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />
            </Menu.Item>
            <Menu.Item key="4">
                <CopyLinkAction assertion={assertion} />
            </Menu.Item>
            <Menu.Item key="5">
                <CopyUrnAction assertion={assertion} />
            </Menu.Item>
            <Menu.Item key="6">
                <DeleteAction
                    assertion={assertion}
                    monitor={monitor}
                    canEdit={monitor ? canEditAssertion && canEditMonitor : canEditAssertion}
                    refetch={refetch}
                />
            </Menu.Item>
        </Menu>
    );
    return (
        <ActionList>
            <StartStopAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />

            <SubscribeAction assertion={assertion} refetch={refetch} />

            <Dropdown overlay={menu} trigger={['click']}>
                <Button type="text" icon={<MoreOutlined />} />
            </Dropdown>
        </ActionList>
    );
};
