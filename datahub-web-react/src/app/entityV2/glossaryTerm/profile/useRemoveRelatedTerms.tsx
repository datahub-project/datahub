import { Modal, message } from 'antd';
import { useTranslation } from 'react-i18next';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveRelatedTermsMutation } from '@graphql/glossaryTerm.generated';
import { DataHubPageModuleType, TermRelationshipType } from '@types';

function useRemoveRelatedTerms(termUrn: string, relationshipType: TermRelationshipType, displayName: string) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();
    const refetch = useRefetch();

    const [removeRelatedTerms] = useRemoveRelatedTermsMutation();

    function handleRemoveRelatedTerms() {
        removeRelatedTerms({
            variables: {
                input: {
                    urn,
                    termUrns: [termUrn],
                    relationshipType,
                },
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('glossaryTerm.removeError', { error: e.message || '' }), duration: 3 });
            })
            .finally(() => {
                message.loading({
                    content: t('glossaryTerm.removing'),
                    duration: 2,
                });
                setTimeout(() => {
                    refetch();
                    message.success({
                        content: t('glossaryTerm.removedSuccess'),
                        duration: 2,
                    });
                    // Reload modules
                    // RelatedTerms - update related terms module on term summary tab
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.RelatedTerms),
                    ]);
                }, 2000);
            });
    }

    function onRemove() {
        Modal.confirm({
            title: t('glossaryTerm.removeConfirmTitle', { name: displayName }),
            content: t('glossaryTerm.removeConfirmBody', {
                entityType: entityRegistry.getEntityName(entityType),
            }),
            onOk() {
                handleRemoveRelatedTerms();
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    }

    return { onRemove };
}

export default useRemoveRelatedTerms;
