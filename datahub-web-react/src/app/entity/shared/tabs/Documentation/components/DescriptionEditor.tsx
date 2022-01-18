import React, { useState } from 'react';
import { message, Button } from 'antd';
import { CheckOutlined } from '@ant-design/icons';

import analytics, { EventType, EntityActionType } from '../../../../../analytics';

import StyledMDEditor from '../../../components/styled/StyledMDEditor';
import TabToolbar from '../../../components/styled/TabToolbar';

import { GenericEntityUpdate } from '../../../types';
import { useEntityData, useEntityUpdate, useRefetch } from '../../../EntityContext';

export const DescriptionEditor = ({ onComplete }: { onComplete?: () => void }) => {
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();

    const description = entityData?.editableProperties?.description || entityData?.properties?.description || '';
    const [updatedDescription, setUpdatedDescription] = useState(description);

    const handleSaveDescription = async () => {
        message.loading({ content: 'Saving...' });
        try {
            await updateEntity({
                variables: { urn, input: { editableProperties: { description: updatedDescription || '' } } },
            });
            message.destroy();
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType,
                entityUrn: urn,
            });
            message.success({ content: 'Description Updated', duration: 2 });
            if (onComplete) onComplete();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update description: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    return entityData ? (
        <>
            <TabToolbar>
                <Button type="text" onClick={onComplete}>
                    Cancel
                </Button>
                <Button onClick={handleSaveDescription}>
                    <CheckOutlined /> Save
                </Button>
            </TabToolbar>
            <StyledMDEditor
                value={description}
                onChange={(v) => setUpdatedDescription(v || '')}
                preview="live"
                visiableDragbar={false}
            />
        </>
    ) : null;
};
