import { Button, Form, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useDomainsContext } from '@app/domain/DomainsContext';
import { useRefetch } from '@app/entity/shared/EntityContext';
import DomainParentSelect from '@app/entity/shared/EntityDropdown/DomainParentSelect';
import { useHandleMoveDomainComplete } from '@app/entity/shared/EntityDropdown/useHandleMoveDomainComplete';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
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
                message.loading({ content: tf('updating'), duration: 2 });
                const newParentToUpdate = selectedParentUrn || undefined;
                handleMoveDomainComplete(domainUrn, newParentToUpdate);
                setTimeout(() => {
                    message.success({
                        content: t('move.success', {
                            entityName: entityRegistry.getEntityName(EntityType.Domain),
                        }),
                        duration: 2,
                    });
                    refetch();
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('move.error', { errorMessage: e.message || '' }), duration: 3 });
            });
        onClose();
    }

    return (
        <Modal
            title={tc('move')}
            data-testid="move-domain-modal"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button onClick={moveDomain} data-testid="move-domain-modal-move-button">
                        {tc('move')}
                    </Button>
                </>
            }
        >
            <Form form={form} initialValues={{}} layout="vertical">
                <Form.Item
                    label={
                        <Typography.Text strong>
                            <Trans t={t} i18nKey="move.toLabel" components={{ optional: <OptionalWrapper /> }} />
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
