import { EditOutlined } from '@ant-design/icons';
import { Collapse, Form, Input, Typography, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useEffect, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entityV2/shared/EntityDropdown/NodeParentSelect';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import DescriptionModal from '@app/entityV2/shared/components/legacy/DescriptionModal';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Modal } from '@src/alchemy-components';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { EntityType } from '@types';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

interface Props {
    entityType: EntityType;
    onClose: () => void;
    refetchData?: () => void;
    // acryl-main only prop
    canCreateGlossaryEntity: boolean;
    canSelectParentUrn?: boolean;
    isCloning?: boolean;
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData, canCreateGlossaryEntity, canSelectParentUrn = true } = props;
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { t: tcl } = useTranslation('common.labels');
    const entityData = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [stagedName, setStagedName] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState(props.isCloning ? '' : entityData.urn);
    const [documentation, setDocumentation] = useState('');
    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);
    const [createButtonDisabled, setCreateButtonDisabled] = useState(true);
    const refetch = useRefetch();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();

    useEffect(() => {
        if (props.isCloning && entityData.entityData) {
            const { properties } = entityData.entityData;

            if (properties?.name) {
                setStagedName(`${properties.name} (copy)`);
                form.setFieldValue('name', `${properties.name} (copy)`);
            }

            if (properties?.description) {
                setDocumentation(properties.description);
            }
        }
    }, [props.isCloning, entityData.entityData, form]);

    function createGlossaryEntity() {
        const mutation =
            entityType === EntityType.GlossaryTerm ? createGlossaryTermMutation : createGlossaryNodeMutation;

        const sanitizedDescription = DOMPurify.sanitize(documentation);
        mutation({
            variables: {
                input: {
                    id: stagedId?.length ? stagedId : undefined,
                    name: stagedName,
                    parentNode: selectedParentUrn || null,
                    description: sanitizedDescription || null,
                },
            },
        })
            .then((result) => {
                message.loading({ content: tcf('updating'), duration: 2 });
                setTimeout(() => {
                    analytics.event({
                        type: EventType.CreateGlossaryEntityEvent,
                        entityType,
                        parentNodeUrn: selectedParentUrn || undefined,
                    });
                    message.success({
                        content: t('createGlossary.success', {
                            entityName: entityRegistry.getEntityName(entityType),
                        }),
                        duration: 2,
                    });
                    refetch();
                    if (isInGlossaryContext) {
                        // either refresh this current glossary node or the root nodes or root terms
                        const nodeToUpdate = selectedParentUrn || getGlossaryRootToUpdate(entityType);
                        updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                        if (selectedParentUrn) {
                            const dataKey =
                                entityType === EntityType.GlossaryTerm ? 'createGlossaryTerm' : 'createGlossaryNode';
                            const newEntityUrn = result.data[dataKey];
                            setNodeToNewEntity((currData) => ({
                                ...currData,
                                [selectedParentUrn]: {
                                    urn: newEntityUrn,
                                    type: entityType,
                                    properties: {
                                        name: stagedName,
                                        description: sanitizedDescription || null,
                                    },
                                },
                            }));
                        }
                    }
                    if (refetchData) {
                        refetchData();
                    }
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('createGlossary.error', { errorMessage: e.message || '' }), duration: 3 });
            });
        onClose();
    }

    function addDocumentation(description: string) {
        setDocumentation(description);
        setIsDocumentationModalVisible(false);
    }

    const entityName =
        !selectedParentUrn && entityType === EntityType.GlossaryNode
            ? t('glossary')
            : entityRegistry.getEntityName(entityType);

    return (
        <Modal
            title={t('createGlossary.title', { entityName })}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('create'),
                    onClick: createGlossaryEntity,
                    variant: 'filled',
                    disabled: createButtonDisabled || !canCreateGlossaryEntity,
                    buttonDataTestId: 'glossary-entity-modal-create-button',
                },
            ]}
            onCancel={onClose}
        >
            <Form
                form={form}
                initialValues={{
                    parent: selectedParentUrn,
                }}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonDisabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>{tcl('name')}</Typography.Text>}>
                    <StyledItem
                        data-testid="create-glossary-entity-modal-name"
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: t('createGlossary.nameRequired', {
                                    entityName: entityRegistry.getEntityName(entityType),
                                }),
                            },
                            { whitespace: true },
                            { min: 1, max: 100 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            autoFocus
                            placeholder={t('createGlossary.namePlaceholder')}
                            value={stagedName}
                            onChange={(event) => setStagedName(event.target.value)}
                        />
                    </StyledItem>
                </Form.Item>
                {canSelectParentUrn && (
                    <Form.Item
                        label={
                            <Typography.Text strong>
                                <Trans
                                    t={t}
                                    i18nKey="createGlossary.parentLabel"
                                    components={{ optional: <OptionalWrapper /> }}
                                />
                            </Typography.Text>
                        }
                    >
                        <StyledItem name="parent">
                            <NodeParentSelect
                                selectedParentUrn={selectedParentUrn}
                                setSelectedParentUrn={setSelectedParentUrn}
                            />
                        </StyledItem>
                    </Form.Item>
                )}

                <StyledItem
                    label={
                        <Typography.Text strong>
                            <Trans
                                t={t}
                                i18nKey="createGlossary.documentationLabel"
                                components={{ optional: <OptionalWrapper /> }}
                            />
                        </Typography.Text>
                    }
                >
                    <Button variant="text" onClick={() => setIsDocumentationModalVisible(true)}>
                        <EditOutlined />
                        {documentation ? t('createGlossary.editDocumentation') : t('createGlossary.addDocumentation')}
                    </Button>
                    {isDocumentationModalVisible && (
                        <DescriptionModal
                            title={t('createGlossary.addDocumentation')}
                            onClose={() => setIsDocumentationModalVisible(false)}
                            onSubmit={addDocumentation}
                            description={documentation}
                        />
                    )}
                </StyledItem>
                <Collapse ghost>
                    <Collapse.Panel
                        header={<Typography.Text type="secondary">{t('createGlossary.advanced')}</Typography.Text>}
                        key="1"
                    >
                        <Form.Item
                            label={
                                <Typography.Text strong>
                                    {t('createGlossary.idLabel', {
                                        entityName: entityRegistry.getEntityName(props.entityType),
                                    })}
                                </Typography.Text>
                            }
                        >
                            <Typography.Paragraph>{t('createGlossary.idHelp')}</Typography.Paragraph>
                            <Form.Item
                                name="id"
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error(t('createGlossary.idInvalid')));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    placeholder={t('createGlossary.idPlaceholder')}
                                    onChange={(event) => setStagedId(event.target.value)}
                                />
                            </Form.Item>
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}

export default CreateGlossaryEntityModal;
