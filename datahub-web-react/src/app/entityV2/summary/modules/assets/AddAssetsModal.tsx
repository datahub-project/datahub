import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityContext, useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { SearchSelectModal } from '@app/entityV2/shared/components/styled/search/SearchSelectModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useBatchAddTermsMutation, useBatchSetDomainMutation } from '@graphql/mutations.generated';
import { EntityType } from '@types';

interface Props {
    setShowAddAssetsModal: React.Dispatch<React.SetStateAction<boolean>>;
}

export default function AddAssetsModal({ setShowAddAssetsModal }: Props) {
    const { t } = useTranslation('modules');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { entityType, urn } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const { setShouldRefetchEmbeddedListSearch, entityState } = useEntityContext();
    const refetch = useRefetch();

    const [isBatchAddGlossaryTermModalVisible, setIsBatchAddGlossaryTermModalVisible] = useState(false);
    const [isBatchSetDomainModalVisible, setIsBatchSetDomainModalVisible] = useState(false);
    const [isBatchSetDataProductModalVisible, setIsBatchSetDataProductModalVisible] = useState(false);
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();

    useEffect(() => {
        if (entityType === EntityType.DataProduct) {
            setIsBatchSetDataProductModalVisible(true);
        } else if (entityType === EntityType.Domain) {
            setIsBatchSetDomainModalVisible(true);
        } else if (entityType === EntityType.GlossaryTerm) {
            setIsBatchAddGlossaryTermModalVisible(true);
        }
    }, [entityType]);

    const batchAddGlossaryTerms = (entityUrns: Array<string>) => {
        batchAddTermsMutation({
            variables: {
                input: {
                    termUrns: [urn],
                    resources: entityUrns.map((entityUrn) => ({
                        resourceUrn: entityUrn,
                    })),
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsBatchAddGlossaryTermModalVisible(false);
                    message.loading({ content: tf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('assets.addedTermSuccess'),
                            duration: 2,
                        });
                        refetch?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                    }, 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: t('assets.addTermError', { error: e.message || '' }),
                        duration: 3,
                    }),
                );
            })
            .finally(() => {
                setShowAddAssetsModal(false);
            });
    };

    const batchSetDomain = (entityUrns: Array<string>) => {
        batchSetDomainMutation({
            variables: {
                input: {
                    domainUrn: urn,
                    resources: entityUrns.map((entityUrn) => ({
                        resourceUrn: entityUrn,
                    })),
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsBatchSetDomainModalVisible(false);
                    message.loading({ content: tf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('assets.addedToDomainSuccess'),
                            duration: 3,
                        });
                        refetch?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                        entityState?.setShouldRefetchContents(true);
                    }, 3000);
                    analytics.event({
                        type: EventType.BatchEntityActionEvent,
                        actionType: EntityActionType.SetDomain,
                        entityUrns,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: t('assets.addToDomainError', { error: e.message || '' }),
                        duration: 3,
                    }),
                );
            })
            .finally(() => {
                setShowAddAssetsModal(false);
            });
    };

    const batchSetDataProduct = (entityUrns: Array<string>) => {
        batchSetDataProductMutation({
            variables: {
                input: {
                    dataProductUrn: urn,
                    resourceUrns: entityUrns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsBatchSetDataProductModalVisible(false);
                    message.loading({ content: tf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('assets.addedToDataProductSuccess'),
                            duration: 3,
                        });
                        refetch?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                    }, 3000);
                    analytics.event({
                        type: EventType.BatchEntityActionEvent,
                        actionType: EntityActionType.SetDataProduct,
                        entityUrns,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: t('assets.addToDataProductError'),
                        duration: 3,
                    }),
                );
            })
            .finally(() => {
                setShowAddAssetsModal(false);
            });
    };

    return (
        <>
            {isBatchAddGlossaryTermModalVisible && (
                <SearchSelectModal
                    titleText={t('assets.addTermModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchAddGlossaryTerms}
                    onCancel={() => setIsBatchAddGlossaryTermModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.GLOSSARY_TERMS),
                    )}
                />
            )}
            {isBatchSetDomainModalVisible && (
                <SearchSelectModal
                    titleText={t('assets.addToDomainModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchSetDomain}
                    onCancel={() => setIsBatchSetDomainModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DOMAINS),
                    )}
                />
            )}
            {isBatchSetDataProductModalVisible && (
                <SearchSelectModal
                    titleText={t('assets.addToDataProductModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchSetDataProduct}
                    onCancel={() => setIsBatchSetDataProductModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DATA_PRODUCTS),
                    )}
                />
            )}
        </>
    );
}
