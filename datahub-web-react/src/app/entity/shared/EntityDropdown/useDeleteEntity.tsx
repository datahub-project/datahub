import { useState } from 'react';
import { message, Modal } from 'antd';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDeleteEntityMutation } from '../../../shared/deleteUtils';
import analytics, { EventType } from '../../../analytics';

/**
 * Performs the flow for deleting an entity of a given type.
 *
 * @param urn the type of the entity to delete
 * @param type the type of the entity to delete
 * @param name the name of the entity to delete
 */
function useDeleteEntity(urn: string, type: EntityType, entityData: any, onDelete?: () => void) {
    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
    const entityRegistry = useEntityRegistry();

    const maybeDeleteEntity = getDeleteEntityMutation(type)();
    const deleteEntity = (maybeDeleteEntity && maybeDeleteEntity[0]) || undefined;

    function handleDeleteEntity() {
        deleteEntity?.({
            variables: {
                urn,
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteEntityEvent,
                    entityUrn: urn,
                    entityType: type,
                });
                message.loading({
                    content: 'Deleting...',
                    duration: 2,
                });
                setTimeout(() => {
                    setHasBeenDeleted(true);
                    onDelete?.();
                    message.success({
                        content: `Deleted ${entityRegistry.getEntityName(type)}!`,
                        duration: 2,
                    });
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to delete: \n ${e.message || ''}`, duration: 3 });
            });
    }

    function onDeleteEntity() {
        Modal.confirm({
            title: `Delete ${
                (entityData && entityRegistry.getDisplayName(type, entityData)) || entityRegistry.getEntityName(type)
            }`,
            content: `Are you sure you want to remove this ${entityRegistry.getEntityName(type)}?`,
            onOk() {
                handleDeleteEntity();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    }

    return { onDeleteEntity, hasBeenDeleted };
}

export default useDeleteEntity;
