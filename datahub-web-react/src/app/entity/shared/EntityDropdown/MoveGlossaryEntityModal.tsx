import { Button, Form, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import NodeParentSelect from '@app/entity/shared/EntityDropdown/NodeParentSelect';
import { useGlossaryEntityData } from '@app/entity/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getModalDomContainer } from '@utils/focus';

import { useUpdateParentNodeMutation } from '@graphql/glossary.generated';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

interface Props {
    onClose: () => void;
}

function MoveGlossaryEntityModal(props: Props) {
    const { onClose } = props;
    const { urn: entityDataUrn, entityData, entityType } = useEntityData();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [selectedParentUrn, setSelectedParentUrn] = useState('');
    const refetch = useRefetch();

    const [updateParentNode] = useUpdateParentNodeMutation();

    function moveGlossaryEntity() {
        updateParentNode({
            variables: {
                input: {
                    resourceUrn: entityDataUrn,
                    parentNode: selectedParentUrn || null,
                },
            },
        })
            .then(() => {
                message.loading({ content: 'Updating...', duration: 2 });
                setTimeout(() => {
                    message.success({
                        content: `Moved ${entityRegistry.getEntityName(entityType)}!`,
                        duration: 2,
                    });
                    refetch();
                    if (isInGlossaryContext) {
                        const oldParentToUpdate = getParentNodeToUpdate(entityData, entityType);
                        const newParentToUpdate = selectedParentUrn || getGlossaryRootToUpdate(entityType);
                        updateGlossarySidebar([oldParentToUpdate, newParentToUpdate], urnsToUpdate, setUrnsToUpdate);
                    }
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to move: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    }

    return (
        <Modal
            data-testid="move-glossary-entity-modal"
            title="Move"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={moveGlossaryEntity} data-testid="glossary-entity-modal-move-button">
                        Move
                    </Button>
                </>
            }
            getContainer={getModalDomContainer}
        >
            <Form form={form} initialValues={{}} layout="vertical">
                <Form.Item
                    label={
                        <Typography.Text strong>
                            Move To <OptionalWrapper>(optional)</OptionalWrapper>
                        </Typography.Text>
                    }
                >
                    <StyledItem name="parent">
                        <NodeParentSelect
                            selectedParentUrn={selectedParentUrn}
                            setSelectedParentUrn={setSelectedParentUrn}
                            isMoving
                            autofocus
                        />
                    </StyledItem>
                </Form.Item>
            </Form>
        </Modal>
    );
}

export default MoveGlossaryEntityModal;
