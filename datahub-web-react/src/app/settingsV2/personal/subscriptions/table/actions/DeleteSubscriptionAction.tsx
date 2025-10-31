import { DeleteOutlined } from '@ant-design/icons';
import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import analytics from '@app/analytics';
import { EventType } from '@app/analytics/event';
import { ActionItem } from '@app/shared/actions';
import useDeleteSubscription from '@app/shared/subscribe/useDeleteSubscription';

import { DataHubSubscription } from '@types';

const StyledDeleteOutlined = styled(DeleteOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    subscription: DataHubSubscription;
    refetchListSubscriptions: (urnsToExclude?: string[]) => void;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const DeleteSubscriptionAction = ({
    subscription,
    refetchListSubscriptions,
    isExpandedView = false,
    onActionTriggered,
}: Props) => {
    const handleRefetch = () => {
        refetchListSubscriptions([subscription.urn]);
    };
    const deleteSubscription = useDeleteSubscription({
        subscription,
        onRefetch: handleRefetch,
    });

    const onDeleteSubscription = () => {
        Modal.confirm({
            title: `Confirm Subscription Removal`,
            content: `Are you sure you want to remove this subscription?`,
            onOk() {
                analytics.event({
                    type: EventType.SubscriptionDeleteClickEvent,
                    subscriptionUrn: subscription.subscriptionUrn,
                });
                deleteSubscription();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ActionItem
            key="delete-subscription"
            tip="Delete this subscription"
            disabled={false}
            onClick={onDeleteSubscription}
            icon={<StyledDeleteOutlined />}
            isExpandedView={isExpandedView}
            actionName="Delete"
            onActionTriggered={onActionTriggered}
        />
    );
};
