import { toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useHandleDeleteDomain } from '@app/entityV2/shared/EntityDropdown/useHandleDeleteDomain';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossaryV2/utils';
import { getDeleteEntityMutation } from '@app/shared/deleteUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
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
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { reloadByKeyType } = useReloadableContext();
    const [hasBeenDeleted, setHasBeenDeleted] = useState(false);
    const [isDeleteModalVisible, setIsDeleteModalVisible] = useState(false);
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
                    toast.loading(t('delete.loading'), { duration: 2 });
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
                            toast.success(
                                t('delete.success', {
                                    entityName: entityRegistry.getEntityName(type),
                                }),
                                { duration: 2 },
                            );
                        }

                        // Reload modules
                        // DataProducts - as listed data product could be removed
                        if (type === EntityType.DataProduct) {
                            reloadByKeyType([
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.DataProducts,
                                ),
                            ]);
                        }
                        // ChildHierarchy - as listed term in contents module in glossary node could be removed
                        // RelatedTerms - as listed term in related terms could be removed
                        if (type === EntityType.GlossaryTerm) {
                            reloadByKeyType([
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.ChildHierarchy,
                                ),
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.RelatedTerms,
                                ),
                            ]);
                        }
                        // ChildHierarchy - as listed node in contents module in glossary node could be removed
                        if (type === EntityType.GlossaryNode) {
                            reloadByKeyType([
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.ChildHierarchy,
                                ),
                            ]);
                        }
                        // ChildHierarchy - as listed domain in child domains module could be removed
                        if (type === EntityType.Domain) {
                            reloadByKeyType([
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.ChildHierarchy,
                                ),
                            ]);
                        }
                    },
                    skipWait ? 0 : 2000,
                );
            })
            .catch((e) => {
                toast.destroy();
                toast.error(t('delete.error', { errorMessage: e.message || '' }), { duration: 3 });
            });
    }

    function onDeleteEntity() {
        setIsDeleteModalVisible(true);
    }

    const DeleteConfirmationModal = (
        <ConfirmationModal
            isOpen={isDeleteModalVisible}
            handleClose={() => setIsDeleteModalVisible(false)}
            handleConfirm={() => {
                setIsDeleteModalVisible(false);
                handleDeleteEntity();
            }}
            modalTitle={t('delete.confirmTitle', {
                entityName:
                    (entityData && entityRegistry.getDisplayName(type, entityData)) ||
                    entityRegistry.getEntityName(type),
            })}
            modalText={t('delete.confirmContent', { entityName: entityRegistry.getEntityName(type) })}
            isDeleteModal
        />
    );

    return { onDeleteEntity, hasBeenDeleted, DeleteConfirmationModal };
}

export default useDeleteEntity;
