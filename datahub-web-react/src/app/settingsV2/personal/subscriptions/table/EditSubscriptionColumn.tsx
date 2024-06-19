import React, { useState } from 'react';
import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { DataHubSubscription, EntityType } from '../../../../../types.generated';
import SubscriptionDrawer from '../../../../shared/subscribe/drawer/SubscriptionDrawer';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import useDeleteSubscription from '../../../../shared/subscribe/useDeleteSubscription';

const EditSubscriptionColumnContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    float: right;
`;

const EditButton = styled(Button)`
    &&:hover {
        background: none;
    }
`;

const EditIcon = styled(EditOutlined)`
    color: ${(props) => props.theme.styles['primary-color']};
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

    const deleteSubscription = useDeleteSubscription({
        subscription,
        isPersonal,
        onRefetch: refetchListSubscriptions,
    });

    const onClickEdit = () => setDrawerIsOpen(true);
    const onClickClose = () => setDrawerIsOpen(false);

    return (
        <EditSubscriptionColumnContainer>
            <EditButton type="text" onClick={onClickEdit}>
                <EditIcon />
            </EditButton>
            <SubscriptionDrawer
                isOpen={drawerIsOpen}
                onClose={onClickClose}
                isPersonal={isPersonal}
                groupUrn={groupUrn}
                entityUrn={entityUrn}
                entityName={entityName}
                entityType={entityType}
                isSubscribed
                canManageSubscription
                subscription={subscription}
                onRefetch={refetchListSubscriptions}
                onDeleteSubscription={deleteSubscription}
            />
        </EditSubscriptionColumnContainer>
    );
}
