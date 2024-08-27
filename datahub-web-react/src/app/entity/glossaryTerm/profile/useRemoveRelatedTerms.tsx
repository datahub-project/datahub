import { message, Modal } from 'antd';
import { useTranslation } from 'react-i18next';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData, useRefetch } from '../../shared/EntityContext';
import { useRemoveRelatedTermsMutation } from '../../../../graphql/glossaryTerm.generated';
import { TermRelationshipType } from '../../../../types.generated';

function useRemoveRelatedTerms(termUrn: string, relationshipType: TermRelationshipType, displayName: string) {
    const { t } = useTranslation();
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
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
                message.error({ content: `${t('crud.error.removeWithName')}: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.loading({
                    content: t('crud.removing'),
                    duration: 2,
                });
                setTimeout(() => {
                    refetch();
                    message.success({
                        content: t('crud.success.removeWithName', { name: t('common.glossaryTerms') }),
                        duration: 2,
                    });
                }, 2000);
            });
    }

    function onRemove() {
        Modal.confirm({
            title: `${t('common.remove')} ${displayName}`,
            content: t('crud.doYouWantTo.removeContentWithTheName', { name: entityRegistry.getEntityName(entityType) }),
            onOk() {
                handleRemoveRelatedTerms();
            },
            onCancel() {},
            okText: t('common.yes'),
            maskClosable: true,
            closable: true,
        });
    }

    return { onRemove };
}

export default useRemoveRelatedTerms;
