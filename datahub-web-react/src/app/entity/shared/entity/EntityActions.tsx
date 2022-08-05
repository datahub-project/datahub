import React, { useState } from 'react';
import { Button, message } from 'antd';
import { LinkOutlined } from '@ant-design/icons';
import { SearchSelectModal } from '../components/styled/search/SearchSelectModal';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityCapabilityType } from '../../Entity';
import { useBatchAddTermsMutation, useBatchSetDomainMutation } from '../../../../graphql/mutations.generated';

export enum EntityActionItem {
    /**
     * Batch add a Glossary Term to a set of assets
     */
    BATCH_ADD_GLOSSARY_TERM,
    /**
     * Batch add a Domain to a set of assets
     */
    BATCH_ADD_DOMAIN,
}

interface Props {
    urn: string;
    actionItems: Set<EntityActionItem>;
    refetchForEntity?: () => void;
}

function EntityActions(props: Props) {
    // eslint ignore react/no-unused-prop-types
    const entityRegistry = useEntityRegistry();
    const { urn, actionItems, refetchForEntity } = props;
    const [isBatchAddGlossaryTermModalVisible, setIsBatchAddGlossaryTermModalVisible] = useState(false);
    const [isBatchSetDomainModalVisible, setIsBatchSetDomainModalVisible] = useState(false);
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchSetDomainMutation] = useBatchSetDomainMutation();

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
                    refetchForEntity?.();
                    setIsBatchAddGlossaryTermModalVisible(false);
                    message.success({
                        content: `Added Glossary Term to entities!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to add glossary term: \n ${e.message || ''}`, duration: 3 });
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
                    refetchForEntity?.();
                    setIsBatchSetDomainModalVisible(false);
                    message.success({
                        content: `Added assets to Domain!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to add assets to Domain: \n ${e.message || ''}`, duration: 3 });
            });
    };

    return (
        <>
            <div style={{ marginRight: 12 }}>
                {actionItems.has(EntityActionItem.BATCH_ADD_GLOSSARY_TERM) && (
                    <Button onClick={() => setIsBatchAddGlossaryTermModalVisible(true)}>
                        <LinkOutlined /> Add to assets
                    </Button>
                )}
                {actionItems.has(EntityActionItem.BATCH_ADD_DOMAIN) && (
                    <Button onClick={() => setIsBatchSetDomainModalVisible(true)}>
                        <LinkOutlined /> Add assets
                    </Button>
                )}
            </div>
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
        </>
    );
}

export default EntityActions;
