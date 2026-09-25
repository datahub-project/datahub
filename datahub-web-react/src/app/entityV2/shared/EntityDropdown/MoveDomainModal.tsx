import { Form, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useDomainsContext } from '@app/domainV2/DomainsContext';
import { useRefetch } from '@app/entity/shared/EntityContext';
import DomainParentSelect from '@app/entityV2/shared/EntityDropdown/DomainParentSelect';
import { useHandleMoveDomainComplete } from '@app/entityV2/shared/EntityDropdown/useHandleMoveDomainComplete';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@src/alchemy-components';

import { useMoveDomainMutation } from '@graphql/domain.generated';
import { DataHubPageModuleType, EntityType } from '@types';

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
    const { t: tcf } = useTranslation('common.feedback');
    const { entityData } = useDomainsContext();
    const domainUrn = entityData?.urn;
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [selectedParentUrn, setSelectedParentUrn] = useState('');
    const refetch = useRefetch();
    const { reloadByKeyType } = useReloadableContext();

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
                message.loading({ content: tcf('updating'), duration: 2 });
                const newParentToUpdate = selectedParentUrn || undefined;
                handleMoveDomainComplete(newParentToUpdate);
                setTimeout(() => {
                    message.success({
                        content: t('move.success', {
                            entityName: entityRegistry.getEntityName(EntityType.Domain),
                        }),
                        duration: 2,
                    });
                    refetch();
                    // Reload modules
                    // ChildHierarchy - as module in domain summary tab could be updated
                    reloadByKeyType([
                        getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.ChildHierarchy),
                    ]);
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
            title={t('moveDomain.title')}
            data-testid="move-domain-modal"
            open
            onCancel={onClose}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('move'),
                    variant: 'filled',
                    onClick: moveDomain,
                    buttonDataTestId: 'move-domain-modal-move-button',
                },
            ]}
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
