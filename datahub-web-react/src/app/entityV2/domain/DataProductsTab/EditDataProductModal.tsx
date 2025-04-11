import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';
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
                    message.success('Updates Data Product!');
                    if (data?.updateDataProduct) {
                        onUpdateDataProduct(data.updateDataProduct as DataProduct);
                    }
                    onClose();
                }
            })
            .catch(() => {
                onClose();
                message.destroy();
                message.error({ content: 'Failed to update Data Product. An unexpected error occurred' });
            });
    }

    return (
        <Modal
            title={`Update ${dataProduct.properties?.name || 'Data Product'}`}
            onCancel={onClose}
            style={MODAL_BODY_STYLE}
            width={MODAL_WIDTH}
            open
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={updateDataProduct} disabled={!builderState.name}>
                        Update
                    </Button>
                </>
            }
        >
            <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
        </Modal>
    );
}
