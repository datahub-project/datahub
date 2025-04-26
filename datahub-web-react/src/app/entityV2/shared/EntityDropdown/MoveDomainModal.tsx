import { Form, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useDomainsContext } from '@app/domainV2/DomainsContext';
import { useRefetch } from '@app/entity/shared/EntityContext';
import DomainParentSelect from '@app/entityV2/shared/EntityDropdown/DomainParentSelect';
import { useHandleMoveDomainComplete } from '@app/entityV2/shared/EntityDropdown/useHandleMoveDomainComplete';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';

import { useMoveDomainMutation } from '@graphql/domain.generated';
import { EntityType } from '@types';

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
            title="Move Domain"
            data-testid="move-domain-modal"
            visible
            onCancel={onClose}
            footer={
                <ModalButtonContainer>
                    <Button variant="text" onClick={onClose}>
                        Cancel
                    </Button>
                    <Button onClick={moveDomain} data-testid="move-domain-modal-move-button">
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
