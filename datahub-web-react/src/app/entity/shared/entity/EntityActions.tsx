import React, { useState } from 'react';
import { Button, message } from 'antd';
import { LinkOutlined } from '@ant-design/icons';
import { EntityType, PlatformPrivileges } from '../../../../types.generated';
import { SearchSelectModal } from '../components/styled/search/SearchSelectModal';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityCapabilityType } from '../../Entity';

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
    entityType: EntityType;
    entityData?: any;
    actionItems: Set<EntityActionItem>;
    platformPrivileges?: PlatformPrivileges;
    refetchForEntity?: () => void;
}

function EntityActions(props: Props) {
    // eslint ignore react/no-unused-prop-types
    const entityRegistry = useEntityRegistry();
    const { urn, entityType, entityData, actionItems, platformPrivileges, refetchForEntity } = props;
    console.log(platformPrivileges);
    console.log(urn);
    console.log(entityType);
    console.log(entityData);

    const [isBatchAddGlossaryTermModalVisible, setIsBatchAddGlossaryTermModalVisible] = useState(false);
    const [isBatchSetDomainModalVisible, setIsBatchSetDomainModalVisible] = useState(false);

    const batchAddGlossaryTerms = (entityUrns: Array<string>) => {
        console.log(`Should be adding terms ${entityUrns}`);
        refetchForEntity?.();
        setIsBatchAddGlossaryTermModalVisible(false);
        message.success('Successfully added glossary terms!');
    };

    const batchSetDomains = (entityUrns: Array<string>) => {
        console.log(`Should be setting domain ${entityUrns}`);
        refetchForEntity?.();
        setIsBatchSetDomainModalVisible(false);
        message.success('Successfully added assets!');
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
                    onContinue={batchSetDomains}
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
