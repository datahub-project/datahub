import { message, Modal } from 'antd';
import { useState } from 'react';
import { useEntityData } from '../EntityContext';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useDeleteGlossaryEntityMutation } from '../../../../graphql/glossary.generated';

function useDeleteGlossaryEntity() {
    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
    const { entityData, urn: entityDataUrn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const [deleteGlossaryEntity] = useDeleteGlossaryEntityMutation();

    function handleDeleteGlossaryEntity() {
        deleteGlossaryEntity({
            variables: {
                urn: entityDataUrn,
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to delete: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.loading({
                    content: 'Deleting...',
                    duration: 2,
                });
                setTimeout(() => {
                    setHasBeenDeleted(true);
                    message.success({
                        content: `Deleted ${entityRegistry.getEntityName(entityType)}!`,
                        duration: 2,
                    });
                }, 2000);
            });
    }

    function onDeleteEntity() {
        Modal.confirm({
            title: `Delete ${entityRegistry.getDisplayName(entityType, entityData)}`,
            content: `Are you sure you want to remove this ${entityRegistry.getEntityName(entityType)}?`,
            onOk() {
                handleDeleteGlossaryEntity();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    }

    return { onDeleteEntity, hasBeenDeleted };
}

export default useDeleteGlossaryEntity;
