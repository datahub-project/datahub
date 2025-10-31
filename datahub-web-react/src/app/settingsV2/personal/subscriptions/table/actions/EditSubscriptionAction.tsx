import { EditOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics from '@app/analytics';
import { EventType } from '@app/analytics/event';
import { ActionItem } from '@app/shared/actions';
import SubscriptionDrawer from '@app/shared/subscribe/drawer/SubscriptionDrawer';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataHubSubscription, EntityType } from '@types';

const StyledEditOutlined = styled(EditOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    subscription: DataHubSubscription;
    refetchListSubscriptions: () => void;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const EditSubscriptionAction = ({
    subscription,
    refetchListSubscriptions,
    isExpandedView = false,
    onActionTriggered,
}: Props) => {
    const [drawerIsOpen, setDrawerIsOpen] = useState(false);
    const { entity } = subscription;
    const entityRegistry = useEntityRegistry();
    const entityType: EntityType = entity.type;
    const entityUrn = entity.urn;
    const entityName: string = entityRegistry.getDisplayName(entityType, entity);

    const onClickEdit = () => {
        analytics.event({
            type: EventType.SubscriptionEditClickEvent,
            subscriptionUrn: subscription.subscriptionUrn,
        });
        setDrawerIsOpen(true);
    };
    const onClickClose = () => setDrawerIsOpen(false);

    // NOTE: We are sort of "hacking" the SubscriptionDrawer. It was sort of built
    // with the assumption that the subscription editor and the subscription owner
    // are the same person, but we are allowing a user to manage their group's
    // subscription here
    const subscriptionOwnerIsGroup = subscription.actor.__typename === 'CorpGroup';
    const groupUrn = subscriptionOwnerIsGroup ? subscription.actorUrn : undefined;

    return (
        <>
            <ActionItem
                key="edit-subscription"
                tip="Edit this subscription"
                disabled={false}
                onClick={onClickEdit}
                icon={<StyledEditOutlined />}
                isExpandedView={isExpandedView}
                actionName="Edit"
                onActionTriggered={onActionTriggered}
            />
            <SubscriptionDrawer
                isOpen={drawerIsOpen}
                onClose={onClickClose}
                isPersonal={!subscriptionOwnerIsGroup}
                groupUrn={groupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed
                canManageSubscription
                subscription={subscription}
                onRefetch={refetchListSubscriptions}
            />
        </>
    );
};
