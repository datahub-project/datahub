import { Form, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import DataProductParentSelect from '@app/entityV2/shared/EntityDropdown/DataProductParentSelect';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@src/alchemy-components';

import { useMoveDataProductMutation } from '@graphql/dataProduct.generated';
import { EntityType } from '@types';

const StyledItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

type Props = {
    onClose: () => void;
};

export default function MoveDataProductModal({ onClose }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { urn: dataProductUrn } = useEntityData();
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const [selectedParentUrn, setSelectedParentUrn] = useState('');
    const refetch = useRefetch();

    const [moveDataProductMutation] = useMoveDataProductMutation();

    async function moveDataProduct() {
        if (!dataProductUrn) return;

        try {
            await moveDataProductMutation({
                variables: {
                    input: {
                        resourceUrn: dataProductUrn,
                        parentDataProduct: selectedParentUrn || undefined,
                    },
                },
            });
            message.loading({ content: tcf('updating'), duration: 2 });
            setTimeout(() => {
                message.success({
                    content: t('move.success', {
                        entityName: entityRegistry.getEntityName(EntityType.DataProduct),
                    }),
                    duration: 2,
                });
                refetch();
            }, 2000);
        } catch (e) {
            message.destroy();
            message.error({
                content: t('move.error', { errorMessage: e instanceof Error ? e.message : '' }),
                duration: 3,
            });
        }
        onClose();
    }

    return (
        <Modal
            title={t('moveDataProduct.title')}
            data-testid="move-data-product-modal"
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
                    onClick: moveDataProduct,
                    buttonDataTestId: 'move-data-product-modal-move-button',
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
                        <DataProductParentSelect
                            selectedParentUrn={selectedParentUrn}
                            setSelectedParentUrn={setSelectedParentUrn}
                            excludeUrn={dataProductUrn}
                        />
                    </StyledItem>
                </Form.Item>
            </Form>
        </Modal>
    );
}
