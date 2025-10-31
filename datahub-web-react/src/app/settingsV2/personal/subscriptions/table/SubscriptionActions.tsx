import { Dropdown } from 'antd';
import { DotsThreeVertical } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DeleteSubscriptionAction } from '@app/settingsV2/personal/subscriptions/table/actions/DeleteSubscriptionAction';
import { EditSubscriptionAction } from '@app/settingsV2/personal/subscriptions/table/actions/EditSubscriptionAction';
import { Button, colors } from '@src/alchemy-components';

import { DataHubSubscription } from '@types';

const ActionList = styled.div<{ $shouldRightAlign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: end;
    padding: ${(props) => (props.$shouldRightAlign ? '0px' : '0px 10px')};
    margin-left: ${(props) => (props.$shouldRightAlign ? 'auto' : undefined)};
    gap: 8px;
`;

type Props = {
    subscription: DataHubSubscription;
    refetchListSubscriptions: () => void;
    shouldRightAlign?: boolean;
};

export const SubscriptionActions = ({ subscription, refetchListSubscriptions, shouldRightAlign }: Props) => {
    const [dropdownOpen, setDropdownOpen] = useState(false);

    const handleMenuClick = () => {
        setDropdownOpen(false);
    };

    const menuItems = [
        {
            key: '1',
            label: (
                <EditSubscriptionAction
                    subscription={subscription}
                    refetchListSubscriptions={refetchListSubscriptions}
                    isExpandedView
                    onActionTriggered={handleMenuClick}
                />
            ),
        },
        {
            key: '2',
            label: (
                <DeleteSubscriptionAction
                    subscription={subscription}
                    refetchListSubscriptions={refetchListSubscriptions}
                    isExpandedView
                    onActionTriggered={handleMenuClick}
                />
            ),
        },
    ];

    return (
        <ActionList onClick={(e) => e.stopPropagation()} $shouldRightAlign={shouldRightAlign}>
            <Dropdown
                menu={{ items: menuItems }}
                trigger={['click']}
                open={dropdownOpen}
                onOpenChange={setDropdownOpen}
            >
                <Button variant="text">
                    <DotsThreeVertical size={20} color={colors.gray[500]} weight="bold" />
                </Button>
            </Dropdown>
        </ActionList>
    );
};
