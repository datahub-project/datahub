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
    refetchListSubscriptions: () => void;
    isPersonal: boolean;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const DeleteSubscriptionAction = ({
    subscription,
    refetchListSubscriptions,
    isPersonal,
    isExpandedView = false,
    onActionTriggered,
}: Props) => {
    const deleteSubscription = useDeleteSubscription({
        subscription,
        isPersonal,
        onRefetch: refetchListSubscriptions,
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

    const authorizedTip = 'Delete this subscription';

    return (
        <ActionItem
            key="delete-subscription"
            tip={authorizedTip}
            disabled={false}
            onClick={onDeleteSubscription}
            icon={<StyledDeleteOutlined />}
            isExpandedView={isExpandedView}
            actionName="Delete"
            onActionTriggered={onActionTriggered}
        />
    );
};
