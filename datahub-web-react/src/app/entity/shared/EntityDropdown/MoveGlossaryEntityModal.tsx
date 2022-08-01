import React, { useState, useEffect } from 'react';
import styled from 'styled-components/macro';
import { message, Button, Modal, Typography, Form } from 'antd';
import { useEntityData, useRefetch } from '../EntityContext';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useUpdateParentNodeMutation } from '../../../../graphql/glossary.generated';
import NodeParentSelect from './NodeParentSelect';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

interface Props {
    onClose: () => void;
    refetchData?: () => void;
}

function MoveGlossaryEntityModal(props: Props) {
    const { onClose, refetchData } = props;
    const { urn: entityDataUrn, entityType } = useEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [selectedParentUrn, setSelectedParentUrn] = useState('');
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const refetch = useRefetch();

    const [updateParentNode] = useUpdateParentNodeMutation();

    useEffect(() => {
        if (selectedParentUrn) {
            setCreateButtonEnabled(true);
        } else {
            setCreateButtonEnabled(false);
        }
    }, [selectedParentUrn]);

    function moveGlossaryEntity() {
        updateParentNode({
            variables: {
                input: {
                    resourceUrn: entityDataUrn,
                    parentNode: selectedParentUrn,
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
                    if (refetchData) {
                        refetchData();
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
            title="Move"
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={moveGlossaryEntity} disabled={!createButtonEnabled}>
                        Move
                    </Button>
                </>
            }
        >
            <Form form={form} initialValues={{}} layout="vertical">
                <Form.Item label={<Typography.Text strong>Move To</Typography.Text>}>
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
