import { Form, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import NodeParentSelect from '@app/entityV2/shared/EntityDropdown/NodeParentSelect';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getGlossaryRootToUpdate, getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';

import { useUpdateParentNodeMutation } from '@graphql/glossary.generated';
import { EntityType } from '@types';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

interface Props {
    entityData: GenericEntityProperties | null;
    entityType: EntityType;
    urn: string;
    onClose: () => void;
}

function MoveGlossaryEntityModal({ onClose, urn, entityData, entityType }: Props) {
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
                    resourceUrn: urn,
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
            title={`Move ${entityType === EntityType.GlossaryNode ? 'Term Group' : 'Term'}`}
            visible
            onCancel={onClose}
            footer={
                <ModalButtonContainer>
                    <Button onClick={onClose} variant="outline">
                        Cancel
                    </Button>
                    <Button onClick={moveGlossaryEntity} data-testid="glossary-entity-modal-move-button">
                        Move
                    </Button>
                </ModalButtonContainer>
            }
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
                        />
                    </StyledItem>
                </Form.Item>
            </Form>
        </Modal>
    );
}

export default MoveGlossaryEntityModal;
