import { EditOutlined } from '@ant-design/icons';
import { Modal } from '@components';
import { Button, Collapse, Form, Input, Typography, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useEffect, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entity/shared/EntityDropdown/NodeParentSelect';
import DescriptionModal from '@app/entity/shared/components/legacy/DescriptionModal';
import { getEntityPath } from '@app/entity/shared/containers/profile/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { DataHubPageModuleType, EntityType } from '@types';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

const StyledButton = styled(Button)`
    padding: 0;
`;

interface Props {
    entityType: EntityType;
    onClose: () => void;
    refetchData?: () => void;
    isCloning?: boolean;
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData } = props;
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const { t: tcl } = useTranslation('common.labels');
    const entityData = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [stagedName, setStagedName] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(props.isCloning ? '' : entityData.urn);
    const [documentation, setDocumentation] = useState('');
    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);
    const [createButtonDisabled, setCreateButtonDisabled] = useState(true);
    const refetch = useRefetch();
    const history = useHistory();
    const { reloadByKeyType } = useReloadableContext();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();

    useEffect(() => {
        if (props.isCloning && entityData.entityData) {
            const { properties } = entityData.entityData;

            if (properties?.name) {
                setStagedName(properties.name);
                form.setFieldValue('name', properties.name);
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
            .then((res) => {
                message.loading({ content: tf('updating'), duration: 2 });
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
                            const newEntityUrn = res.data[dataKey];
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
                    if (props.isCloning) {
                        const redirectUrn =
                            entityType === EntityType.GlossaryTerm
                                ? res.data?.createGlossaryTerm
                                : res.data?.createGlossaryNode;
                        history.push(getEntityPath(entityType, redirectUrn, entityRegistry, false, false));
                    }
                    // Reload modules
                    // ChildHierarchy - to update contents module as new term/node could change it
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy),
                    ]);
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

    return (
        <Modal
            title={t('createGlossary.title', { entityName: entityRegistry.getEntityName(entityType) })}
            open
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('create'),
                    variant: 'filled',
                    disabled: createButtonDisabled,
                    onClick: createGlossaryEntity,
                    buttonDataTestId: 'glossary-entity-modal-create-button',
                },
            ]}
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
                        <Input autoFocus value={stagedName} onChange={(event) => setStagedName(event.target.value)} />
                    </StyledItem>
                </Form.Item>
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
                    <StyledButton type="link" onClick={() => setIsDocumentationModalVisible(true)}>
                        <EditOutlined />
                        {documentation ? t('createGlossary.editDocumentation') : t('createGlossary.addDocumentation')}
                    </StyledButton>
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
