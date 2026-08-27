import { Modal, Text, toast } from '@components';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import DataProductParentSelect from '@app/entityV2/shared/EntityDropdown/DataProductParentSelect';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useMoveDataProductMutation } from '@graphql/dataProduct.generated';
import { DataProduct, EntityType } from '@types';

const OptionalWrapper = styled.span`
    font-weight: normal;
`;

const Field = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    onClose: () => void;
};

export default function MoveDataProductModal({ onClose }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcf } = useTranslation('common.feedback');
    const { urn: dataProductUrn, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const oldParent = (entityData as DataProduct | null)?.properties?.parentDataProduct;
    const oldParentUrn = oldParent?.urn || '';
    const oldParentName = oldParent?.properties?.name || oldParentUrn;
    const [selectedParentUrn, setSelectedParentUrn] = useState(oldParentUrn);
    const refetch = useRefetch();

    const [moveDataProductMutation, { loading }] = useMoveDataProductMutation();
    const isUnchanged = selectedParentUrn === oldParentUrn;

    async function moveDataProduct() {
        if (!dataProductUrn) return;

        try {
            await moveDataProductMutation({
                variables: {
                    input: {
                        resourceUrn: dataProductUrn,
                        parentDataProduct: selectedParentUrn || null,
                    },
                },
            });
            analytics.event({
                type: EventType.MoveDataProductEvent,
                oldParentDataProductUrn: oldParentUrn || undefined,
                parentDataProductUrn: selectedParentUrn || undefined,
            });
            toast.loading(tcf('updating'), { duration: 2 });
            toast.success(
                t('move.success', {
                    entityName: entityRegistry.getEntityName(EntityType.DataProduct),
                }),
                { duration: 2 },
            );
            refetch();
            onClose();
        } catch (e) {
            toast.error(t('move.error', { errorMessage: e instanceof Error ? e.message : '' }), { duration: 3 });
        }
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
                    disabled: loading,
                },
                {
                    text: tc('move'),
                    variant: 'filled',
                    onClick: moveDataProduct,
                    disabled: loading || isUnchanged,
                    isLoading: loading,
                    buttonDataTestId: 'move-data-product-modal-move-button',
                },
            ]}
        >
            <Field>
                <Text weight="bold">
                    <Trans t={t} i18nKey="move.toLabel" components={{ optional: <OptionalWrapper /> }} />
                </Text>
                <DataProductParentSelect
                    selectedParentUrn={selectedParentUrn}
                    setSelectedParentUrn={setSelectedParentUrn}
                    excludeUrn={dataProductUrn}
                    initialParentName={oldParentName || undefined}
                />
            </Field>
        </Modal>
    );
}
