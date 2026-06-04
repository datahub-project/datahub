import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import DataProductBuilderForm from '@app/entityV2/domain/DataProductsTab/DataProductBuilderForm';
import { DataProductBuilderState } from '@app/entityV2/domain/DataProductsTab/types';

import { useUpdateDataProductMutation } from '@graphql/dataProduct.generated';
import { DataProduct } from '@types';

type Props = {
    dataProduct: DataProduct;
    onClose: () => void;
    onUpdateDataProduct: (dataProduct: DataProduct) => void;
};

export default function EditDataProductModal({ dataProduct, onUpdateDataProduct, onClose }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const [builderState, updateBuilderState] = useState<DataProductBuilderState>({
        name: dataProduct.properties?.name || '',
        description: dataProduct.properties?.description || '',
    });
    const [updateDataProductMutation] = useUpdateDataProductMutation();

    function updateDataProduct() {
        updateDataProductMutation({
            variables: {
                urn: dataProduct.urn,
                input: {
                    name: builderState.name,
                    description: builderState.description || undefined,
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success(t('dataProduct.updateSuccess'));
                    if (data?.updateDataProduct) {
                        onUpdateDataProduct(data.updateDataProduct as DataProduct);
                    }
                    onClose();
                }
            })
            .catch(() => {
                onClose();
                message.destroy();
                message.error({ content: t('dataProduct.updateError') });
            });
    }

    return (
        <Modal
            title={t('dataProduct.updateTitle', {
                name: dataProduct.properties?.name || t('dataProduct.name'),
            })}
            onCancel={onClose}
            open
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                },
                {
                    text: tc('update'),
                    onClick: updateDataProduct,
                    variant: 'filled',
                    disabled: !builderState.name,
                },
            ]}
        >
            <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
        </Modal>
    );
}
