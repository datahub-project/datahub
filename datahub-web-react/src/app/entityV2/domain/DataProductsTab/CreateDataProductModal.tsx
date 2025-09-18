import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';

import DataProductBuilderForm from '@app/entityV2/domain/DataProductsTab/DataProductBuilderForm';
import { DataProductBuilderState } from '@app/entityV2/domain/DataProductsTab/types';
import { useModulesContext } from '@app/homeV3/module/context/ModulesContext';

import { useCreateDataProductMutation } from '@graphql/dataProduct.generated';
import { DataHubPageModuleType, DataProduct, Domain } from '@types';

export const MODAL_WIDTH = '75vw';

export const MODAL_BODY_STYLE = {
    overflow: 'auto',
    width: '80vw',
    maxWidth: 800,
};

const DEFAULT_STATE = {
    name: '',
};

type Props = {
    domain: Domain;
    onClose: () => void;
    onCreateDataProduct: (dataProduct: DataProduct) => void;
};

export default function CreateDataProductModal({ domain, onCreateDataProduct, onClose }: Props) {
    const [builderState, updateBuilderState] = useState<DataProductBuilderState>(DEFAULT_STATE);
    const [createDataProductMutation] = useCreateDataProductMutation();
    const { reloadModules } = useModulesContext();

    function createDataProduct() {
        createDataProductMutation({
            variables: {
                input: {
                    domainUrn: domain.urn,
                    properties: {
                        name: builderState.name,
                        description: builderState.description || undefined,
                    },
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success('Created Data Product!');
                    if (data?.createDataProduct) {
                        const updateDataProduct = { ...data.createDataProduct, domain: { domain } };
                        onCreateDataProduct(updateDataProduct as DataProduct);
                    }
                    onClose();
                    // Reload modules
                    // DataProducts - handling of creating of new data product from data products tab
                    reloadModules([DataHubPageModuleType.DataProducts], 3000);
                }
            })
            .catch(() => {
                onClose();
                message.destroy();
                message.error({ content: 'Failed to create Data Product. An unexpected error occurred' });
            });
    }

    return (
        <Modal
            title="Create Data Product"
            onCancel={onClose}
            style={MODAL_BODY_STYLE}
            width={MODAL_WIDTH}
            open
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" onClick={createDataProduct} disabled={!builderState.name}>
                        Create
                    </Button>
                </>
            }
        >
            <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
        </Modal>
    );
}
