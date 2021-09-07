import { Typography, Button, message } from 'antd';
import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';

import { EntityType, OwnershipUpdate } from '../../../../../../../types.generated';
import analytics, { EntityActionType, EventType } from '../../../../../../analytics';
import { ExpandedOwner } from '../../../../components/styled/ExpandedOwner';

import { EMPTY_MESSAGES } from '../../../../constants';
import { useEntityData, useEntityUpdate } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { AddOwnerModal } from './AddOwnerModal';

export const SidebarOwnerSection = () => {
    const { urn, entityData } = useEntityData();
    const updateEntity = useEntityUpdate();
    const [showAddModal, setShowAddModal] = useState(false);
    const ownersEmpty = !entityData?.ownership?.owners?.length;

    const updateOwnership = (update: OwnershipUpdate) => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateOwnership,
            entityType: EntityType.Dataset,
            entityUrn: urn,
        });
        updateEntity({ variables: { input: { urn, ownership: update } } })
            .then(() => message.success({ content: 'Updated!', duration: 2 }))
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to update: \n ${e.message || ''}`, duration: 3 });
            });
    };

    return (
        <div>
            <SidebarHeader title="Owners" />
            <div>
                {entityData?.ownership?.owners?.map((owner) => (
                    <ExpandedOwner
                        owner={owner}
                        updateOwnership={updateOwnership}
                        owners={entityData.ownership?.owners}
                    />
                ))}
                {ownersEmpty && (
                    <Typography.Paragraph type="secondary">
                        {EMPTY_MESSAGES.owners.title}. {EMPTY_MESSAGES.owners.description}
                    </Typography.Paragraph>
                )}

                <Button type={ownersEmpty ? 'default' : 'text'} onClick={() => setShowAddModal(true)}>
                    <PlusOutlined /> Add Owner
                </Button>
            </div>
            <AddOwnerModal
                owners={entityData?.ownership?.owners}
                updateOwnership={updateOwnership}
                visible={showAddModal}
                onClose={() => {
                    setShowAddModal(false);
                }}
            />
        </div>
    );
};
