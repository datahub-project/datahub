import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { EditOutlined } from '@ant-design/icons';
import { message, Button, Input, Modal, Typography, Form } from 'antd';
import DOMPurify from 'dompurify';
import {
    useCreateGlossaryTermMutation,
    useCreateGlossaryNodeMutation,
} from '../../../../graphql/glossaryTerm.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import NodeParentSelect from './NodeParentSelect';
import { useEntityData, useRefetch } from '../EntityContext';
import analytics, { EventType } from '../../../analytics';
import DescriptionModal from '../components/legacy/DescriptionModal';

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
}

function CreateGlossaryEntityModal(props: Props) {
    const { entityType, onClose, refetchData } = props;
    const entityData = useEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
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
            title={`Create ${entityRegistry.getEntityName(entityType)}`}
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={createGlossaryEntity} disabled={createButtonDisabled}>
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
                        <Input autoFocus value={stagedName} onChange={(event) => setStagedName(event.target.value)} />
                    </StyledItem>
                </Form.Item>
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
                            title="Add Documenataion"
                            onClose={() => setIsDocumentationModalVisible(false)}
                            onSubmit={addDocumentation}
                            description={documentation}
                        />
                    )}
                </StyledItem>
            </Form>
        </Modal>
    );
}

export default CreateGlossaryEntityModal;
