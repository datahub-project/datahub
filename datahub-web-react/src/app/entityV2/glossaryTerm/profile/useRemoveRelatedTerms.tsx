import { Modal, message } from 'antd';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveRelatedTermsMutation } from '@graphql/glossaryTerm.generated';
import { DataHubPageModuleType, TermRelationshipType } from '@types';

function useRemoveRelatedTerms(termUrn: string, relationshipType: TermRelationshipType, displayName: string) {
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
                message.error({ content: `Failed to remove: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.loading({
                    content: 'Removing...',
                    duration: 2,
                });
                setTimeout(() => {
                    refetch();
                    message.success({
                        content: `Removed Glossary Term!`,
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
            title: `Remove ${displayName}`,
            content: `Are you sure you want to remove this ${entityRegistry.getEntityName(entityType)}?`,
            onOk() {
                handleRemoveRelatedTerms();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    }

    return { onRemove };
}

export default useRemoveRelatedTerms;
