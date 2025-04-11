import analytics, { EventType } from '@app/analytics';
import { EntitySearchInputV2 } from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputV2';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Input, Modal } from '@components';
import { Label } from '@components/components/Input/components';
import { useLinkAssetVersionMutation } from '@graphql/versioning.generated';
import { Entity, EntityType } from '@types';
import { Form, message } from 'antd';
import React, { useState } from 'react';
import { v4 as uuidv4 } from 'uuid';

const ENTITY_FIELD_NAME = 'entity';
const LABEL_FIELD_NAME = 'label';
const COMMENT_FIELD_NAME = 'comment';

interface FormType {
    entityUrnToLink?: string;
    label: string;
    comment?: string;
}

interface Props {
    urn: string;
    entityType: EntityType;
    closeModal: () => void;
    refetch?: () => void;
}

export default function LinkAssetVersionModal({ urn, entityType, closeModal, refetch }: Props) {
    const entityRegistry = useEntityRegistry();
    const [link] = useLinkAssetVersionMutation();

    const [form] = Form.useForm<FormType>();
    const formValues = Form.useWatch([], form);
    const [selectedEntity, setSelectedEntity] = useState<Entity | undefined>();

    function close() {
        form.resetFields();
        closeModal();
    }

    function handleLink() {
        form.validateFields().then(() => {
            const versionProperties =
                selectedEntity &&
                entityRegistry.getGenericEntityProperties(selectedEntity.type, selectedEntity)?.versionProperties;

            if (selectedEntity && (!versionProperties?.isLatest || !versionProperties?.versionSet?.urn)) {
                message.error({
                    content: 'Cannot link to a non-versioned or non-latest entity',
                    duration: 3,
                });
                return;
            }

            const versionSetUrn =
                versionProperties?.versionSet?.urn ||
                `urn:li:versionSet:(${uuidv4()},${entityRegistry.getGraphNameFromType(entityType)})`;

            link({
                variables: {
                    input: {
                        linkedEntity: urn,
                        versionSet: versionSetUrn,
                        version: formValues.label,
                        comment: formValues.comment,
                    },
                },
            })
                .then(() => {
                    close();
                    analytics.event({
                        type: EventType.LinkAssetVersionEvent,
                        oldAssetUrn: selectedEntity?.urn,
                        newAssetUrn: urn,
                        versionSetUrn,
                        entityType,
                    });
                    message.loading({
                        content: 'Linking...',
                        duration: 2,
                    });

                    setTimeout(() => {
                        refetch?.();
                        message.success({
                            content: `Linked ${entityRegistry.getEntityName(entityType)}!`,
                            duration: 2,
                        });
                    }, 2000);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to link: \n ${e.message || ''}`, duration: 3 });
                });
        });
    }

    return (
        <Modal
            title="Link a Newer Version"
            onCancel={close}
            buttons={[
                { text: 'Cancel', variant: 'text', onClick: close },
                { text: 'Create', variant: 'filled', onClick: handleLink },
            ]}
        >
            <Form
                form={form}
                layout="vertical"
                onKeyDown={(e) => {
                    // Preventing the modal from closing when the Enter key is pressed
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                    }
                }}
            >
                <Form.Item name={ENTITY_FIELD_NAME}>
                    <Label>Previous Version</Label>
                    <EntitySearchInputV2
                        entityTypes={[entityType]}
                        placeholder={`Select a ${entityRegistry
                            .getEntityName(entityType)
                            ?.toLocaleLowerCase()} to link`}
                        searchPlaceholder={`Search for a ${entityRegistry
                            .getCollectionName(entityType)
                            .toLocaleLowerCase()}`}
                        orFilters={[{ and: [{ field: 'isLatest', values: ['true'] }] }]}
                        onUpdate={setSelectedEntity}
                    />
                </Form.Item>
                <Form.Item
                    name={LABEL_FIELD_NAME}
                    rules={[{ min: 1, max: 100, required: true, message: 'Version name is required' }]}
                    hasFeedback
                >
                    <Input label="Version Name" placeholder="v.2321" />
                </Form.Item>
                <Form.Item name={COMMENT_FIELD_NAME} rules={[{ min: 1, max: 500 }]} hasFeedback>
                    <Input label="Version Note" placeholder="This version does..." />
                </Form.Item>
            </Form>
        </Modal>
    );
}
