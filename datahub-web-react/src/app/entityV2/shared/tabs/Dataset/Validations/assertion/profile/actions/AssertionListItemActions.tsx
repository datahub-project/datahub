import { Dropdown, Menu } from 'antd';
import { DotsThreeVertical } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { useConnectionWithRunAssertionCapabilitiesForEntityExists } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { ContractAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ContractAction';
import { CopyLinkAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyLinkAction';
import { CopyUrnAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/CopyUrnAction';
import { DeleteAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/DeleteAction';
import { ExternalUrlAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ExternalUrlAction';
import { RunAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/RunAction';
import { StartStopAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/StartStopAction';
import { SubscribeAction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/SubscribeAction';
import { useIsOnSiblingsView } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { Button, colors } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useAppConfig } from '@src/app/useAppConfig';

import { Assertion, AssertionRunStatus, AssertionSourceType, DataContract, Monitor } from '@types';

const ActionList = styled.div<{ $shouldRightAlign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: end;
    padding: ${(props) => (props.$shouldRightAlign ? '0px' : '0px 10px')};
    margin-left: ${(props) => (props.$shouldRightAlign ? 'auto' : undefined)};
    gap: 8px;
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
            {isRunAssertionsEnabled && monitor && !isNonNative && isReachable ? (
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
