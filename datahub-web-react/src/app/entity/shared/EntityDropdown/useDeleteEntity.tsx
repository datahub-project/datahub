import { useState } from 'react';
import { message, Modal } from 'antd';
import { useTranslation } from 'react-i18next';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDeleteEntityMutation } from '../../../shared/deleteUtils';
import analytics, { EventType } from '../../../analytics';
import { useGlossaryEntityData } from '../GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '../../../glossary/utils';
import { useHandleDeleteDomain } from './useHandleDeleteDomain';
import { removeTermFromGlossaryNode } from '../../../glossary/cacheUtils';

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
    const { t } = useTranslation();

    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const { handleDeleteDomain } = useHandleDeleteDomain({ entityData, urn });

    const [deleteEntity, { client }] = getDeleteEntityMutation(type)() ?? [undefined, { client: undefined }];

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
                        content: `${t('crud.deleting')}...`,
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
                            if (client) {
                                removeTermFromGlossaryNode(client, parentNodeToUpdate, urn);
                            }
                        }
                        if (!hideMessage) {
                            message.success({
                                content: `${t('crud.deleteWithName', { name: entityRegistry.getEntityName(type) })}!`,
                                duration: 2,
                            });
                        }
                    },
                    skipWait ? 0 : 2000,
                );
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `${t('crud.error.delete')}: \n ${e.message || ''}`, duration: 3 });
            });
    }

    function onDeleteEntity() {
        Modal.confirm({
            title: `${t('crud.delete')} ${
                (entityData && entityRegistry.getDisplayName(type, entityData)) || entityRegistry.getEntityName(type)
            }`,
            content: `${t('crud.doYouWantTo.removeContentWithThisName', {
                name: entityRegistry.getEntityName(type),
            })}?`,
            onOk() {
                handleDeleteEntity();
            },
            onCancel() {},
            okText: t('common.yes'),
            maskClosable: true,
            closable: true,
        });
    }

    return { onDeleteEntity, hasBeenDeleted };
}

export default useDeleteEntity;
