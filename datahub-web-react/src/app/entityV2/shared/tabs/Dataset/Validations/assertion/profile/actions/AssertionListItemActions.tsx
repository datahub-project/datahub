import React from 'react';
import styled from 'styled-components';
<<<<<<< HEAD
import { Button, Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';

import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useAppConfig } from '@src/app/useAppConfig';

import { StartStopAction } from './StartStopAction';
import {
    Assertion,
    AssertionRunStatus,
    AssertionSourceType,
    DataContract,
    Monitor,
} from '../../../../../../../../../types.generated';
import { DeleteAction } from './DeleteAction';
||||||| f14c42d2ef7
import { Button, Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';

import { Assertion, AssertionRunStatus, DataContract } from '../../../../../../../../../types.generated';
=======
import { Dropdown, Menu } from 'antd';
import { Button, colors } from '@src/alchemy-components';
import { DotsThreeVertical } from 'phosphor-react';
import { Assertion, AssertionRunStatus, DataContract } from '../../../../../../../../../types.generated';
>>>>>>> master
import { ContractAction } from './ContractAction';
import { CopyLinkAction } from './CopyLinkAction';
import { CopyUrnAction } from './CopyUrnAction';
import { SubscribeAction } from './SubscribeAction';
import { RunAction } from './RunAction';
import { ExternalUrlAction } from './ExternalUrlAction';
import { useIsOnSiblingsView } from '../../../../../../useIsSeparateSiblingsMode';
import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '../../../acrylUtils';

const ActionList = styled.div<{ $shouldRightAlign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: end;
    padding: ${(props) => (props.$shouldRightAlign ? '0px' : '0px 10px')};
    margin-left: ${(props) => (props.$shouldRightAlign ? 'auto' : undefined)};
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    refetch?: () => void;
    shouldRightAlign?: boolean;
};

export const AssertionListItemActions = ({
    assertion,
    monitor,
    contract,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    refetch,
    shouldRightAlign,
}: Props) => {
    const isSiblingsView = useIsOnSiblingsView();
    const mostRun = assertion.runEvents?.runEvents;
    const externalUrl =
        assertion?.info?.externalUrl ||
        (mostRun?.length && mostRun[0].status === AssertionRunStatus.Complete && mostRun[0].result?.externalUrl);
    const { urn: entityUrn } = useEntityData();
    const { config } = useAppConfig();
    const isRunAssertionsEnabled = config?.featureFlags?.runAssertionsEnabled;
    const isReachable = useConnectionWithRunAssertionCapabilitiesForEntityExists(entityUrn ?? '');
    const isNonNative = assertion.info?.source?.type !== AssertionSourceType.Native;
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
            {isRunAssertionsEnabled && !monitor && !isNonNative && isReachable ? (
                <Menu.Item key="3">
                    <RunAction
                        assertion={assertion}
                        monitor={monitor}
                        canEdit={canEditMonitor}
                        refetch={refetch}
                        isExpandedView
                    />
                </Menu.Item>
            ) : null}

            <Menu.Item key="4">
                <CopyLinkAction assertion={assertion} isExpandedView />
            </Menu.Item>
            <Menu.Item key="5">
                <CopyUrnAction assertion={assertion} isExpandedView />
            </Menu.Item>
            <Menu.Item key="6">
                <DeleteAction
                    assertion={assertion}
                    monitor={monitor}
                    canEdit={monitor ? canEditAssertion && canEditMonitor : canEditAssertion}
                    refetch={refetch}
                    isExpandedView
                />
            </Menu.Item>
        </Menu>
    );
    return (
        <ActionList onClick={(e) => e.stopPropagation()} $shouldRightAlign={shouldRightAlign}>
            <StartStopAction assertion={assertion} monitor={monitor} canEdit={canEditMonitor} refetch={refetch} />

            <SubscribeAction assertion={assertion} refetch={refetch} />

            <Dropdown overlay={menu} trigger={['click']}>
                <Button variant="text">
                    <DotsThreeVertical size={20} color={colors.gray[500]} weight="bold" />
                </Button>
            </Dropdown>
        </ActionList>
    );
};
