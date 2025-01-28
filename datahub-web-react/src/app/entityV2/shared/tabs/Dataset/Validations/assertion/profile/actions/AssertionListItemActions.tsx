import React from 'react';
import styled from 'styled-components';
import { Button, Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';

import { Assertion, AssertionRunStatus, DataContract } from '../../../../../../../../../types.generated';
import { ContractAction } from './ContractAction';
import { CopyLinkAction } from './CopyLinkAction';
import { CopyUrnAction } from './CopyUrnAction';
import { ExternalUrlAction } from './ExternalUrlAction';
import { useIsOnSiblingsView } from '../../../../../../useIsSeparateSiblingsMode';

const ActionList = styled.div<{ $shouldRightAlign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: ${(props) => (props.$shouldRightAlign ? '0px' : '0px 10px')};
    margin-left: ${(props) => (props.$shouldRightAlign ? 'auto' : undefined)};
`;

type Props = {
    assertion: Assertion;
    contract?: DataContract;
    canEditContract: boolean;
    refetch?: () => void;
    shouldRightAlign?: boolean;
};

export const AssertionListItemActions = ({
    assertion,
    contract,
    canEditContract,
    refetch,
    shouldRightAlign,
}: Props) => {
    const isSiblingsView = useIsOnSiblingsView();
    const mostRun = assertion.runEvents?.runEvents;
    const externalUrl =
        assertion?.info?.externalUrl ||
        (mostRun?.length && mostRun[0].status === AssertionRunStatus.Complete && mostRun[0].result?.externalUrl);
    const menu = (
        <Menu>
            {/** Currently, we do not handle adding to a contract in siblings mode, since we only load the root node's contract. */}
            {!isSiblingsView ? (
                <Menu.Item key="1">
                    <ContractAction
                        assertion={assertion}
                        contract={contract}
                        canEdit={canEditContract}
                        refetch={refetch}
                        isExpandedView
                    />
                </Menu.Item>
            ) : null}
            {externalUrl ? (
                <Menu.Item key="2">
                    <ExternalUrlAction assertion={assertion} isExpandedView />
                </Menu.Item>
            ) : null}
            <Menu.Item key="4">
                <CopyLinkAction assertion={assertion} isExpandedView />
            </Menu.Item>
            <Menu.Item key="5">
                <CopyUrnAction assertion={assertion} isExpandedView />
            </Menu.Item>
        </Menu>
    );
    return (
        <ActionList onClick={(e) => e.stopPropagation()} $shouldRightAlign={shouldRightAlign}>
            <Dropdown overlay={menu} trigger={['click']}>
                <Button type="text" icon={<MoreOutlined />} />
            </Dropdown>
        </ActionList>
    );
};
