import React, { useState } from 'react';
import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { DataHubSubscription, EntityType } from '../../../../../types.generated';
import SubscriptionDrawer from '../../../../shared/subscribe/drawer/SubscriptionDrawer';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { useDeleteSubscriptionMutation } from '../../../../../graphql/subscriptions.generated';
import { deleteSubscriptionFunction } from '../../../../shared/subscribe/drawer/utils';

const EditSubscriptionColumnContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    gap: 8px;
`;

interface Props {
    subscription: DataHubSubscription;
    refetchListSubscriptions: () => void;
    isPersonal: boolean;
    groupUrn?: string;
}

export function EditSubscriptionColumn({ subscription, refetchListSubscriptions, isPersonal, groupUrn }: Props) {
    const [drawerIsOpen, setDrawerIsOpen] = useState(false);
    const { entity } = subscription;
    const entityRegistry = useEntityRegistry();
    const entityType: EntityType = entity.type;
    const entityUrn = entity.urn;
    const entityName: string = entityRegistry.getDisplayName(entityType, entity);

    const [deleteSubscription] = useDeleteSubscriptionMutation();
    const onDeleteSubscription = () => {
        if (subscription && subscription.subscriptionUrn) {
            deleteSubscriptionFunction(subscription.subscriptionUrn, deleteSubscription, refetchListSubscriptions);
        }
    };

    return (
        <EditSubscriptionColumnContainer>
            <Button type="text" onClick={() => setDrawerIsOpen(true)}>
                <EditOutlined />
            </Button>
            <SubscriptionDrawer
                isOpen={drawerIsOpen}
                onClose={() => setDrawerIsOpen(false)}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed
                subscription={subscription}
                refetchGetSubscription={refetchListSubscriptions}
                onDeleteSubscription={onDeleteSubscription}
            />
        </EditSubscriptionColumnContainer>
    );
}
