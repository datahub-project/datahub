import { Modal, message } from 'antd';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useHandleDeleteDomain } from '@app/entity/shared/EntityDropdown/useHandleDeleteDomain';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { getDeleteEntityMutation } from '@app/shared/deleteUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

/**
 * Performs the flow for deleting an entity of a given type.
 *
 * @param urn the type of the entity to delete
 * @param type the type of the entity to delete
 * @param name the name of the entity to delete
 */
function useDeleteEntity(
    urn: string,
    type: EntityType,
    entityData: any,
    onDelete?: () => void,
    hideMessage?: boolean,
    skipWait?: boolean,
) {
    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToDeletedUrn } = useGlossaryEntityData();
    const { handleDeleteDomain } = useHandleDeleteDomain({ entityData, urn });

    const [deleteEntity] = getDeleteEntityMutation(type)() ?? [undefined, { client: undefined }];

    function handleDeleteEntity() {
        deleteEntity?.({ variables: { urn } })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteEntityEvent,
                    entityUrn: urn,
                    entityType: type,
                });
                if (!hideMessage && !skipWait) {
                    message.loading({
                        content: t('delete.loading'),
                        duration: 2,
                    });
                }

                if (entityData.type === EntityType.Domain) {
                    handleDeleteDomain();
                }

                setTimeout(
                    () => {
                        setHasBeenDeleted(true);
                        onDelete?.();
                        if (isInGlossaryContext) {
                            const parentNodeToUpdate = getParentNodeToUpdate(entityData, type);
                            updateGlossarySidebar([parentNodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                            setNodeToDeletedUrn((currData) => ({
                                ...currData,
                                [parentNodeToUpdate]: urn,
                            }));
                        }
                        if (!hideMessage) {
                            message.success({
                                content: t('delete.success', {
                                    entityName: entityRegistry.getEntityName(type),
                                }),
                                duration: 2,
                            });
                        }
                    },
                    skipWait ? 0 : 2000,
                );
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('delete.error', { errorMessage: e.message || '' }), duration: 3 });
            });
    }

    function onDeleteEntity() {
        Modal.confirm({
            title: t('delete.confirmTitle', {
                entityName:
                    (entityData && entityRegistry.getDisplayName(type, entityData)) ||
                    entityRegistry.getEntityName(type),
            }),
            content: t('delete.confirmContent', { entityName: entityRegistry.getEntityName(type) }),
            onOk() {
                handleDeleteEntity();
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    }

    return { onDeleteEntity, hasBeenDeleted };
}

export default useDeleteEntity;
