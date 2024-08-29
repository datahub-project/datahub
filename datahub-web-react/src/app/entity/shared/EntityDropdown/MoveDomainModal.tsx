import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { message, Button, Modal, Typography, Form } from 'antd';
import { useRefetch } from '../EntityContext';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useMoveDomainMutation } from '../../../../graphql/domain.generated';
import DomainParentSelect from './DomainParentSelect';
import { useHandleMoveDomainComplete } from './useHandleMoveDomainComplete';
import { useDomainsContext } from '../../../domain/DomainsContext';
import { EntityType } from '../../../../types.generated';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

interface Props {
    onClose: () => void;
}

function MoveDomainModal(props: Props) {
    const { onClose } = props;
    const { entityData } = useDomainsContext();
    const domainUrn = entityData?.urn;
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [selectedParentUrn, setSelectedParentUrn] = useState('');
    const refetch = useRefetch();

    const [moveDomainMutation] = useMoveDomainMutation();

    const { handleMoveDomainComplete } = useHandleMoveDomainComplete();

    function moveDomain() {
        if (!domainUrn) return;

        moveDomainMutation({
            variables: {
                input: {
                    resourceUrn: domainUrn,
                    parentDomain: selectedParentUrn || undefined,
                },
            },
        })
            .then(() => {
                message.loading({ content: 'Updating...', duration: 2 });
                const newParentToUpdate = selectedParentUrn || undefined;
                handleMoveDomainComplete(domainUrn, newParentToUpdate);
                setTimeout(() => {
                    message.success({
                        content: `Moved ${entityRegistry.getEntityName(EntityType.Domain)}!`,
                        duration: 2,
                    });
                    refetch();
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
            data-testid="move-domain-modal"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={moveDomain} data-testid="move-domain-modal-move-button">
                        Move
                    </Button>
                </>
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
                        <DomainParentSelect
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

export default MoveDomainModal;
