import { toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveRelatedTermsMutation } from '@graphql/glossaryTerm.generated';
import { DataHubPageModuleType, TermRelationshipType } from '@types';

function useRemoveRelatedTerms(termUrn: string, relationshipType: TermRelationshipType, displayName: string) {
    const { t } = useTranslation('entity.types');
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();
    const refetch = useRefetch();
    const [showConfirmModal, setShowConfirmModal] = useState(false);

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
                toast.destroy();
                toast.error(t('glossaryTerm.removeError', { error: e.message || '' }), { duration: 3 });
            })
            .finally(() => {
                toast.loading(t('glossaryTerm.removing'), { duration: 2 });
                setTimeout(() => {
                    refetch();
                    toast.success(t('glossaryTerm.removedSuccess'), { duration: 2 });
                    // Reload modules
                    // RelatedTerms - update related terms module on term summary tab
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.RelatedTerms),
                    ]);
                }, 2000);
            });
        setShowConfirmModal(false);
    }

    function onRemove() {
        setShowConfirmModal(true);
    }

    const removeConfirmationModal = (
        <ConfirmationModal
            isOpen={showConfirmModal}
            handleClose={() => setShowConfirmModal(false)}
            handleConfirm={handleRemoveRelatedTerms}
            modalTitle={t('glossaryTerm.removeConfirmTitle', { name: displayName })}
            modalText={t('glossaryTerm.removeConfirmBody', {
                entityType: entityRegistry.getEntityName(entityType),
            })}
        />
    );

    return { onRemove, removeConfirmationModal };
}

export default useRemoveRelatedTerms;
