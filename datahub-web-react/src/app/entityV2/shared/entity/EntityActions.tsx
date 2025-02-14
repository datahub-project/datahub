import React, { useState } from 'react';
import { Button, message } from 'antd';
import { Tooltip } from '@components';
import styled from 'styled-components/macro';
import { LinkOutlined, PlusOutlined } from '@ant-design/icons';
import { SearchSelectModal } from '../components/styled/search/SearchSelectModal';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityCapabilityType } from '../../Entity';
import { useBatchAddTermsMutation, useBatchSetDomainMutation } from '../../../../graphql/mutations.generated';
import { handleBatchError } from '../utils';
import { useBatchSetDataProductMutation } from '../../../../graphql/dataProduct.generated';
import { useEntityContext, useEntityData } from '../../../entity/shared/EntityContext';
import CreateGlossaryEntityModal from '../EntityDropdown/CreateGlossaryEntityModal';
import { EntityType } from '../../../../types.generated';

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

const TransparentButton = styled(Button)`
    color: #533fd1;
    font-size: 12px;
    box-shadow: none;
    border-color: #533fd1;
    padding: 10px 20px;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: #533fd1;
        color: #533fd1;
    }
`;

const PrimaryButton = styled(Button)`
    color: #ffffff;
    font-size: 12px;
    box-shadow: none;
    border-color: #533fd1;
    background-color: #533fd1;
    padding: 10px 20px;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: #533fd1;
        background-color: #533fd1;
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
    const entityRegistry = useEntityRegistry();
    const { urn, actionItems, refetchForEntity, refetchForTerms, refetchForNodes } = props;
    const { setShouldRefetchEmbeddedListSearch, entityState } = useEntityContext();
    const [isBatchAddGlossaryTermModalVisible, setIsBatchAddGlossaryTermModalVisible] = useState(false);
    const [isBatchSetDomainModalVisible, setIsBatchSetDomainModalVisible] = useState(false);
    const [isBatchSetDataProductModalVisible, setIsBatchSetDataProductModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();

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
                    message.loading({ content: 'Updating...', duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: `Added Glossary Term to entities!`,
                            duration: 2,
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
                        content: `Failed to add glossary term: \n ${e.message || ''}`,
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
                    message.loading({ content: 'Updating...', duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: `Added assets to Domain!`,
                            duration: 3,
                        });
                        refetchForEntity?.();
                        setShouldRefetchEmbeddedListSearch?.(true);
                        entityState?.setShouldRefetchContents(true);
                    }, 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(entityUrns, e, {
                        content: `Failed to add assets to Domain: \n ${e.message || ''}`,
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
                    message.loading({ content: 'Updating...', duration: 3 });
                    setTimeout(() => {
                        message.success({
                            content: `Added assets to Data Product!`,
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
                        content: `Failed to add assets to Data Product. An unknown error occurred.`,
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
                    <Tooltip title="Add Glossary Term to Assets" showArrow={false} placement="bottom">
                        <TransparentButton
                            size="large"
                            onClick={() => setIsBatchAddGlossaryTermModalVisible(true)}
                            data-testid="glossary-batch-add"
                        >
                            <LinkOutlined /> Add to Assets
                        </TransparentButton>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_DOMAIN) && (
                    <Tooltip title="Add Assets to Domain" showArrow={false} placement="bottom">
                        <TransparentButton
                            size="large"
                            onClick={() => setIsBatchSetDomainModalVisible(true)}
                            data-testid="domain-batch-add"
                        >
                            <LinkOutlined /> Add to Assets
                        </TransparentButton>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_DATA_PRODUCT) && (
                    <Tooltip
                        title="Add Assets to Data Product"
                        showArrow={false}
                        placement="bottom"
                        data-testid="data-product-batch-add"
                    >
                        <TransparentButton size="large" onClick={() => setIsBatchSetDataProductModalVisible(true)}>
                            <LinkOutlined />
                            Add Assets
                        </TransparentButton>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.ADD_CHILD_GLOSSARY_NODE) && (
                    <Tooltip title="Create New Term Group" showArrow={false} placement="bottom">
                        <TransparentButton
                            data-testid="add-term-group-button-v2"
                            size="large"
                            onClick={() => setIsCreateNodeModalVisible(true)}
                        >
                            <StyledPlusOutlined /> Add Term Group
                        </TransparentButton>
                    </Tooltip>
                )}
                {actionItems.has(EntityActionItem.ADD_CHILD_GLOSSARY_TERM) && (
                    <Tooltip title="Create New Glossary Term" showArrow={false} placement="bottom">
                        <PrimaryButton
                            data-testid="add-term-button"
                            type="primary"
                            size="large"
                            onClick={() => setIsCreateTermModalVisible(true)}
                        >
                            <StyledPlusOutlined /> Add Term
                        </PrimaryButton>
                    </Tooltip>
                )}
            </ButtonWrapper>
            {isBatchAddGlossaryTermModalVisible && (
                <SearchSelectModal
                    titleText="Add Glossary Term to assets"
                    continueText="Add"
                    onContinue={batchAddGlossaryTerms}
                    onCancel={() => setIsBatchAddGlossaryTermModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.GLOSSARY_TERMS),
                    )}
                />
            )}
            {isBatchSetDomainModalVisible && (
                <SearchSelectModal
                    titleText="Add assets to Domain"
                    continueText="Add"
                    onContinue={batchSetDomain}
                    onCancel={() => setIsBatchSetDomainModalVisible(false)}
                    fixedEntityTypes={Array.from(
                        entityRegistry.getTypesWithSupportedCapabilities(EntityCapabilityType.DOMAINS),
                    )}
                />
            )}
            {isBatchSetDataProductModalVisible && (
                <SearchSelectModal
                    titleText="Add assets to Data Product"
                    continueText="Add"
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
