import { Input, Modal } from '@components';
import { Form, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { v4 as uuidv4 } from 'uuid';

import { Label } from '@components/components/Input/components';

import analytics, { EventType } from '@app/analytics';
import { EntitySearchInputV2 } from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputV2';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useLinkAssetVersionMutation } from '@graphql/versioning.generated';
import { Entity, EntityType } from '@types';

const ENTITY_FIELD_NAME = 'entity';
const LABEL_FIELD_NAME = 'label';
const COMMENT_FIELD_NAME = 'comment';
// GraphQL search filter restricting selection to the latest version of an entity.
const LATEST_VERSION_FILTERS = [{ and: [{ field: 'isLatest', values: ['true'] }] }];

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
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
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
                    content: t('linkVersion.invalidError'),
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
                        content: t('linkVersion.loading'),
                        duration: 2,
                    });

                    setTimeout(() => {
                        refetch?.();
                        message.success({
                            content: t('linkVersion.success', {
                                entityName: entityRegistry.getEntityName(entityType),
                            }),
                            duration: 2,
                        });
                    }, 2000);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: t('linkVersion.error', { errorMessage: e.message || '' }), duration: 3 });
                });
        });
    }

    return (
        <Modal
            title={t('linkVersion.title')}
            onCancel={close}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: close, key: 'Cancel' },
                { text: tc('create'), variant: 'filled', onClick: handleLink, key: 'Create' },
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
                    <Label>{t('linkVersion.previousVersionLabel')}</Label>
                    <EntitySearchInputV2
                        entityTypes={[entityType]}
                        placeholder={t('linkVersion.selectPlaceholder', {
                            entityName: entityRegistry.getEntityName(entityType)?.toLocaleLowerCase(),
                        })}
                        searchPlaceholder={t('linkVersion.searchPlaceholder', {
                            collectionName: entityRegistry.getCollectionName(entityType).toLocaleLowerCase(),
                        })}
                        orFilters={LATEST_VERSION_FILTERS}
                        onUpdate={setSelectedEntity}
                    />
                </Form.Item>
                <Form.Item
                    name={LABEL_FIELD_NAME}
                    rules={[{ min: 1, max: 100, required: true, message: t('linkVersion.nameRequired') }]}
                    hasFeedback
                >
                    <Input label={t('linkVersion.nameLabel')} placeholder={t('linkVersion.namePlaceholder')} />
                </Form.Item>
                <Form.Item name={COMMENT_FIELD_NAME} rules={[{ min: 1, max: 500 }]} hasFeedback>
                    <Input label={t('linkVersion.noteLabel')} placeholder={t('linkVersion.notePlaceholder')} />
                </Form.Item>
            </Form>
        </Modal>
    );
}
