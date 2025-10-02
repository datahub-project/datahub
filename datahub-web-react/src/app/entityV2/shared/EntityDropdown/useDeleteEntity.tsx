import { Modal, message } from 'antd';
import { useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useHandleDeleteDomain } from '@app/entityV2/shared/EntityDropdown/useHandleDeleteDomain';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossaryV2/utils';
import { getReloadableModuleKey } from '@app/homeV3/modules/utils';
import { getDeleteEntityMutation } from '@app/shared/deleteUtils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataHubPageModuleType, EntityType } from '@types';

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
    const { reloadByKeyType } = useReloadableContext();
    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
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
                        content: 'Deleting...',
                        duration: 2,
                    });
                }

                if (type === EntityType.Domain) {
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
                                content: `Deleted ${entityRegistry.getEntityName(type)}!`,
                                duration: 2,
                            });
                        }

                        // Reload modules
                        // DataProducts - as listed data product could be removed
                        if (type === EntityType.DataProduct) {
                            reloadByKeyType([getReloadableModuleKey(DataHubPageModuleType.DataProducts)]);
                        }
                        // ChildHierarchy - as listed term in contents module in glossary node could be removed
                        // RelatedTerms - as listed term in related terms could be removed
                        if (type === EntityType.GlossaryTerm) {
                            reloadByKeyType([
                                getReloadableModuleKey(DataHubPageModuleType.ChildHierarchy),
                                getReloadableModuleKey(DataHubPageModuleType.RelatedTerms),
                            ]);
                        }
                        // ChildHierarchy - as listed node in contents module in glossary node could be removed
                        if (type === EntityType.GlossaryNode) {
                            reloadByKeyType([getReloadableModuleKey(DataHubPageModuleType.ChildHierarchy)]);
                        }
                        // ChildHierarchy - as listed domain in child domains module could be removed
                        if (type === EntityType.Domain) {
                            reloadByKeyType([getReloadableModuleKey(DataHubPageModuleType.ChildHierarchy)]);
                        }
                    },
                    skipWait ? 0 : 2000,
                );
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
