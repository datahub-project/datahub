import { message, Modal } from 'antd';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useEntityData } from '../EntityContext';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useDeleteGlossaryEntityMutation } from '../../../../graphql/glossary.generated';

function useDeleteGlossaryEntity() {
    const { t } = useTranslation();
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
                message.error({ content: `${t('crud.error.delete')} \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.loading({
                    content: t('crud.deleting'),
                    duration: 2,
                });
                setTimeout(() => {
                    setHasBeenDeleted(true);
                    message.success({
                        content: `${t('common.deleted')} ${entityRegistry.getEntityName(entityType)}!`,
                        duration: 2,
                    });
                }, 2000);
            });
    }

    function onDeleteEntity() {
        Modal.confirm({
            title: `${t('crud.delete')} ${entityRegistry.getDisplayName(entityType, entityData)}`,
            content: `${t('crud.doYouWantTo.removeContentWithThisName')} ${entityRegistry.getEntityName(entityType)}?`,
            onOk() {
                handleDeleteGlossaryEntity();
            },
            onCancel() {},
            okText: t('common.yes'),
            maskClosable: true,
            closable: true,
        });
    }

    return { onDeleteEntity, hasBeenDeleted };
}

export default useDeleteGlossaryEntity;
