import { LinkOutlined, PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { SearchSelectModal } from '@app/entityV2/shared/components/styled/search/SearchSelectModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useBatchSetApplicationMutation } from '@graphql/application.generated';
import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';
import { useBatchAddTermsMutation, useBatchSetDomainMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType } from '@types';

export enum EntityActionItem {
    /**
     * Batch add a Glossary Term to a set of assets
     */
    BATCH_ADD_GLOSSARY_TERM,
    /**
     * Batch add a Domain to a set of assets
     */
    BATCH_ADD_DOMAIN,
    /**
     * Batch add a Data Product to a set of assets
     */
    BATCH_ADD_DATA_PRODUCT,
    /**
     * Add a new Glossary Term as child
     */
    ADD_CHILD_GLOSSARY_TERM,
    /**
     * Add a new Glossary Node as child
     */
    ADD_CHILD_GLOSSARY_NODE,
    /**
     * Batch add an Application to a set of assets
     */
    BATCH_ADD_APPLICATION,
}

const ButtonWrapper = styled.div`
    gap: 8px;
    white-space: nowrap;
    display: flex;
    align-items: center;
    overflow: hidden;

    & button:hover {
        opacity: 0.9;
    }
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    font-size: 12px;
`;

interface Props {
    urn: string;
    actionItems: Set<EntityActionItem>;
    refetchForEntity?: () => void;
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
}

function EntityActions(props: Props) {
    // eslint ignore react/no-unused-prop-types
    const { t } = useTranslation('entity.shared.actions');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const entityRegistry = useEntityRegistry();
    const { urn, actionItems, refetchForEntity, refetchForTerms, refetchForNodes } = props;
    const { setShouldRefetchEmbeddedListSearch, entityState } = useEntityContext();
    const [isBatchAddGlossaryTermModalVisible, setIsBatchAddGlossaryTermModalVisible] = useState(false);
    const [isBatchSetDomainModalVisible, setIsBatchSetDomainModalVisible] = useState(false);
    const [isBatchSetDataProductModalVisible, setIsBatchSetDataProductModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isBatchSetApplicationModalVisible, setIsBatchSetApplicationModalVisible] = useState(false);
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();
    const { reloadByKeyType } = useReloadableContext();

    // eslint-disable-next-line
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
                    message.loading({ content: tcf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('addedTermSuccess'),
                            duration: 2,
                        });
                        refetchForEntity?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                        // Reload modules
                        // Assets - to reload shown related assets on asset summary tab
                        reloadByKeyType([
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                        ]);
                    }, 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: t('addTermError', { error: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    // eslint-disable-next-line
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
                    message.loading({ content: tcf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('addedDomainSuccess'),
                            duration: 3,
                        });
                        refetchForEntity?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                        entityState?.setShouldRefetchContents(true);
                        // Reload modules
                        // Assets - to reload shown related assets on asset summary tab
                        // Domains - to reload Domains module with top domains on home page as list of domains can be changed after adding assets
                        reloadByKeyType([
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Domains),
                        ]);
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
                        content: t('addDomainError', { error: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    // eslint-disable-next-line
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
                    message.loading({ content: tcf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('addedDataProductSuccess'),
                            duration: 3,
                        });
                        refetchForEntity?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                        // Reload modules
                        // Assets - to reload shown related assets on asset summary tab
                        reloadByKeyType([
                            getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets),
                        ]);
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
                        content: t('addDataProductError'),
                        duration: 3,
                    }),
                );
            });
    };

    const batchSetApplication = (entityUrns: Array<string>) => {
        batchSetApplicationMutation({
            variables: {
                input: {
                    applicationUrn: urn,
                    resourceUrns: entityUrns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    setIsBatchSetApplicationModalVisible(false);
                    message.loading({ content: tcf('updating'), duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: t('addedApplicationSuccess'),
                            duration: 3,
                        });
                        refetchForEntity?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                    }, 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: t('addApplicationError'),
                        duration: 3,
                    }),
                );
            });
    };

    const { entityData } = useEntityData();
    const canCreateGlossaryEntity = !!entityData?.privileges?.canManageChildren;

    return (
        <>
            <ButtonWrapper>
                {actionItems.has(EntityActionItem.BATCH_ADD_GLOSSARY_TERM) && (
                    <Tooltip title={t('addTermTooltip')} showArrow={false} placement="bottom">
                        <Button
                            variant="outline"
                            onClick={() => setIsBatchAddGlossaryTermModalVisible(true)}
                            data-testid="glossary-batch-add"
                            size="sm"
                        >
                            <LinkOutlined /> {t('addToAssets')}
                        </Button>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_DOMAIN) && (
                    <Tooltip title={t('addDomainTooltip')} showArrow={false} placement="bottom">
                        <Button
                            variant="outline"
                            onClick={() => setIsBatchSetDomainModalVisible(true)}
                            data-testid="domain-batch-add"
                            size="sm"
                        >
                            <LinkOutlined /> {t('addToAssets')}
                        </Button>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_DATA_PRODUCT) && (
                    <Tooltip
                        title={t('addDataProductTooltip')}
                        showArrow={false}
                        placement="bottom"
                        data-testid="data-product-batch-add"
                    >
                        <Button
                            variant="outline"
                            onClick={() => setIsBatchSetDataProductModalVisible(true)}
                            size="sm"
                            data-testid="data-product-batch-add"
                        >
                            <LinkOutlined />
                            {t('addAssets')}
                        </Button>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.ADD_CHILD_GLOSSARY_NODE) && (
                    <Tooltip title={t('createTermGroupTooltip')} showArrow={false} placement="bottom">
                        <Button
                            data-testid="add-term-group-button-v2"
                            variant="outline"
                            onClick={() => setIsCreateNodeModalVisible(true)}
                        >
                            <StyledPlusOutlined /> {t('addTermGroup')}
                        </Button>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.ADD_CHILD_GLOSSARY_TERM) && (
                    <Tooltip title={t('createTermTooltip')} showArrow={false} placement="bottom">
                        <Button data-testid="add-term-button" onClick={() => setIsCreateTermModalVisible(true)}>
                            <StyledPlusOutlined /> {t('addTerm')}
                        </Button>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_APPLICATION) && (
                    <Tooltip title={t('addApplicationTooltip')} showArrow={false} placement="bottom">
                        <Button variant="outline" onClick={() => setIsBatchSetApplicationModalVisible(true)}>
                            <LinkOutlined /> {t('addToAssets')}
                        </Button>
                    </Tooltip>
                )}
            </ButtonWrapper>
            {isBatchAddGlossaryTermModalVisible && (
                <SearchSelectModal
                    titleText={t('addTermModalTitle')}
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
                    titleText={t('addDomainModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchSetDomain}
                    onCancel={() => setIsBatchSetDomainModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DOMAINS),
                    )}
                />
            )}
            {isBatchSetApplicationModalVisible && (
                <SearchSelectModal
                    titleText={t('addApplicationModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchSetApplication}
                    onCancel={() => setIsBatchSetApplicationModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.APPLICATIONS),
                    )}
                />
            )}
            {isBatchSetDataProductModalVisible && (
                <SearchSelectModal
                    titleText={t('addDataProductModalTitle')}
                    continueText={tc('add')}
                    onContinue={batchSetDataProduct}
                    onCancel={() => setIsBatchSetDataProductModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DATA_PRODUCTS),
                    )}
                />
            )}
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    canCreateGlossaryEntity={canCreateGlossaryEntity}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={canCreateGlossaryEntity}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
        </>
    );
}

export default EntityActions;
