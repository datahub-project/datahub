import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import DataProductBuilderForm from './DataProductBuilderForm';
import { DataProductBuilderState } from './types';
import { useUpdateDataProductMutation } from '../../../../graphql/dataProduct.generated';
import { DataProduct } from '../../../../types.generated';
import { MODAL_BODY_STYLE, MODAL_WIDTH } from './CreateDataProductModal';

type Props = {
    dataProduct: DataProduct;
    onClose: () => void;
    onUpdateDataProduct: (dataProduct: DataProduct) => void;
};

export default function EditDataProductModal({ dataProduct, onUpdateDataProduct, onClose }: Props) {
    const { t } = useTranslation();
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
                    message.success(t('shared.dataProductUpdate'));
                    if (data?.updateDataProduct) {
                        onUpdateDataProduct(data.updateDataProduct as DataProduct);
                    }
                    onClose();
                }
            })
            .catch(() => {
                onClose();
                message.destroy();
                message.error({ content: t('crud.error.failedToDeleteDataProduct') });
            });
    }

    return (
        <Modal
            title={`${t('common.refresh')} ${dataProduct.properties?.name || t('common.dataProduct')}`}
            onCancel={onClose}
            style={MODAL_BODY_STYLE}
            width={MODAL_WIDTH}
            open
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button onClick={updateDataProduct} disabled={!builderState.name}>
                        {t('common.update')}
                    </Button>
                </>
            }
        >
            <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
        </Modal>
    );
}
