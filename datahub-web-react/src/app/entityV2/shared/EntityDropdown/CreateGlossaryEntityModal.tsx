import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EditOutlined } from '@ant-design/icons';
import { message, Button, Input, Modal, Typography, Form, Collapse } from 'antd';
import DOMPurify from 'dompurify';
import {
    useCreateGlossaryTermMutation,
    useCreateGlossaryNodeMutation,
} from '../../../../graphql/glossaryTerm.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import NodeParentSelect from './NodeParentSelect';
import { useEntityData, useRefetch } from '../../../entity/shared/EntityContext';
import analytics, { EventType } from '../../../analytics';
import DescriptionModal from '../components/legacy/DescriptionModal';
import { validateCustomUrnId } from '../../../shared/textUtil';
import { useGlossaryEntityData } from '../GlossaryEntityContext';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '../../../glossary/utils';

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
    // acryl-main only prop
    canCreateGlossaryEntity: boolean;
    canSelectParentUrn?: boolean;
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData, canCreateGlossaryEntity, canSelectParentUrn = true } = props;
    const entityData = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [stagedName, setStagedName] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState(entityData.urn);
    const [documentation, setDocumentation] = useState('');
    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);
    const [createButtonDisabled, setCreateButtonDisabled] = useState(true);
    const refetch = useRefetch();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();

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
            .then(() => {
                message.loading({ content: 'Updating...', duration: 2 });
                setTimeout(() => {
                    analytics.event({
                        type: EventType.CreateGlossaryEntityEvent,
                        entityType,
                        parentNodeUrn: selectedParentUrn || undefined,
                    });
                    message.success({
                        content: `Created ${entityRegistry.getEntityName(entityType)}!`,
                        duration: 2,
                    });
                    refetch();
                    if (isInGlossaryContext) {
                        // either refresh this current glossary node or the root nodes or root terms
                        const nodeToUpdate = entityData?.urn || getGlossaryRootToUpdate(entityType);
                        updateGlossarySidebar([nodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                    }
                    if (refetchData) {
                        refetchData();
                    }
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    }

    function addDocumentation(description: string) {
        setDocumentation(description);
        setIsDocumentationModalVisible(false);
    }

    return (
        <Modal
            title={`Create ${
                !selectedParentUrn && entityType === EntityType.GlossaryNode
                    ? 'Glossary'
                    : entityRegistry.getEntityName(entityType)
            }`}
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button
                        type="primary"
                        data-testid="glossary-entity-modal-create-button"
                        onClick={createGlossaryEntity}
                        disabled={createButtonDisabled || !canCreateGlossaryEntity}
                    >
                        Create
                    </Button>
                </>
            }
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
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <StyledItem
                        data-testid="create-glossary-entity-modal-name"
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: `Enter a ${entityRegistry.getEntityName(entityType)} name.`,
                            },
                            { whitespace: true },
                            { min: 1, max: 100 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            autoFocus
                            placeholder="Provide a name..."
                            value={stagedName}
                            onChange={(event) => setStagedName(event.target.value)}
                        />
                    </StyledItem>
                </Form.Item>
                {canSelectParentUrn && (
                    <Form.Item
                        label={
                            <Typography.Text strong>
                                Parent <OptionalWrapper>(optional)</OptionalWrapper>
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
                            Documentation <OptionalWrapper>(optional)</OptionalWrapper>
                        </Typography.Text>
                    }
                >
                    <StyledButton type="link" onClick={() => setIsDocumentationModalVisible(true)}>
                        <EditOutlined />
                        {documentation ? 'Edit' : 'Add'} Documentation
                    </StyledButton>
                    {isDocumentationModalVisible && (
                        <DescriptionModal
                            title="Add Documentation"
                            onClose={() => setIsDocumentationModalVisible(false)}
                            onSubmit={addDocumentation}
                            description={documentation}
                        />
                    )}
                </StyledItem>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item
                            label={
                                <Typography.Text strong>
                                    {entityRegistry.getEntityName(props.entityType)} Id
                                </Typography.Text>
                            }
                        >
                            <Typography.Paragraph>
                                By default, a random UUID will be generated to uniquely identify this entity. If
                                you&apos;d like to provide a custom id, you may provide it here. Note that it should be
                                unique across the entire Glossary. Be careful, you cannot easily change the id after
                                creation.
                            </Typography.Paragraph>
                            <Form.Item
                                name="id"
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error('Please enter a valid entity id'));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    placeholder="classification"
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
