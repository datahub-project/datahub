import { Collapse, Form, message } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { Label } from '@components/components/TextArea/components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossaryNodeSelector from '@app/entityV2/shared/GlossaryNodeSelector/GlossaryNodeSelector';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { getGlossaryRootToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { validateCustomUrnId } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Editor, Input, Modal } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { useCreateGlossaryNodeMutation, useCreateGlossaryTermMutation } from '@graphql/glossaryTerm.generated';
import { EntityType } from '@types';

const FormItem = styled(Form.Item)`
    .ant-form-item-label {
        padding-bottom: 2px;
    }
`;

const FormItemWithMargin = styled(FormItem)`
    margin-bottom: 16px;
`;

const OwnersContainer = styled.div`
    margin-bottom: 16px;
`;

const StyledLabel = styled(Label)`
    font-size: 12px;
    font-weight: 700;
    color: ${colors.gray[600]};
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${colors.gray[1400]};
    border-radius: 8px;
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
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate, setNodeToNewEntity } = useGlossaryEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [stagedName, setStagedName] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState(entityData.urn);
    const [description, setDescription] = useState('');
    const [createButtonDisabled, setCreateButtonDisabled] = useState(true);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const { user } = useUserContext();
    const refetch = useRefetch();

    const [createGlossaryTermMutation] = useCreateGlossaryTermMutation();
    const [createGlossaryNodeMutation] = useCreateGlossaryNodeMutation();

    function createGlossaryEntity() {
        const mutation =
            entityType === EntityType.GlossaryTerm ? createGlossaryTermMutation : createGlossaryNodeMutation;

        const sanitizedDescription = description ? DOMPurify.sanitize(description) : null;
        const ownerInputs = createOwnerInputs(selectedOwnerUrns);

        mutation({
            variables: {
                input: {
                    id: stagedId?.length ? stagedId : undefined,
                    name: stagedName,
                    parentNode: selectedParentUrn || null,
                    description: sanitizedDescription,
                    owners: ownerInputs.length > 0 ? ownerInputs : undefined,
                },
            },
        })
            .then((result) => {
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
                                        description: sanitizedDescription,
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
                message.error({ content: `Failed to create: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    }

    return (
        <Modal
            title={`Create ${
                !selectedParentUrn && entityType === EntityType.GlossaryNode
                    ? 'Glossary'
                    : entityRegistry.getEntityName(entityType)
            }`}
            visible
            width={752}
            onCancel={onClose}
            footer={
                <ModalButtonContainer>
                    <Button color="gray" onClick={onClose} variant="text">
                        Cancel
                    </Button>
                    <Button
                        data-testid="glossary-entity-modal-create-button"
                        onClick={createGlossaryEntity}
                        disabled={createButtonDisabled || !canCreateGlossaryEntity}
                    >
                        Create
                    </Button>
                </ModalButtonContainer>
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
                <FormItemWithMargin
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
                    <StyledLabel>Name</StyledLabel>
                    <Input
                        label=""
                        autoFocus
                        data-testid="create-glossary-entity-modal-name"
                        placeholder="Provide a name..."
                        value={stagedName}
                        onChange={({ target: { value } }) => {
                            setStagedName(value);
                            form.setFieldValue('name', value);
                            form.validateFields(['name']);
                        }}
                    />
                </FormItemWithMargin>

                <FormItemWithMargin>
                    <StyledLabel>Description</StyledLabel>
                    <StyledEditor
                        content={description}
                        onChange={setDescription}
                        placeholder="Add a description for your glossary entity"
                        doNotFocus
                    />
                </FormItemWithMargin>

                {canSelectParentUrn && (
                    <FormItemWithMargin name="parent">
                        <StyledLabel>Parent</StyledLabel>
                        <GlossaryNodeSelector
                            selectedNodes={selectedParentUrn ? [selectedParentUrn] : []}
                            onNodesChange={(nodeUrns) => setSelectedParentUrn(nodeUrns[0] || '')}
                            placeholder="Select parent node"
                            label=""
                            isMultiSelect={false}
                        />
                    </FormItemWithMargin>
                )}

                <OwnersContainer>
                    <StyledLabel>Add Owners</StyledLabel>
                    <ActorsSearchSelect
                        selectedActorUrns={selectedOwnerUrns}
                        onUpdate={(actors) => setSelectedOwnerUrns(actors.map((actor) => actor.urn))}
                        placeholder="Search for users or groups"
                        defaultActors={user ? [user] : []}
                        width="full"
                    />
                </OwnersContainer>
                <Collapse ghost>
                    <Collapse.Panel header={<StyledLabel>Advanced Options</StyledLabel>} key="1">
                        <FormItemWithMargin
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
                            <StyledLabel>{entityRegistry.getEntityName(props.entityType)} Id</StyledLabel>
                            <Input
                                label=""
                                placeholder="classification"
                                value={stagedId || ''}
                                onChange={({ target: { value } }) => {
                                    setStagedId(value);
                                    form.setFieldValue('id', value);
                                    form.validateFields(['id']);
                                }}
                                helperText="By default, a random UUID will be generated to uniquely identify this entity. If you'd like to provide a custom id, you may provide it here. Note that it should be unique across the entire Glossary. Be careful, you cannot easily change the id after creation."
                            />
                        </FormItemWithMargin>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}

export default CreateGlossaryEntityModal;
