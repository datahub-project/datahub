import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import DataProductBuilderForm from './DataProductBuilderForm';
import { DataProductBuilderState } from './types';
import { useCreateDataProductMutation } from '../../../../graphql/dataProduct.generated';
import { DataProduct, Domain } from '../../../../types.generated';

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
    const { t } = useTranslation();
    const [builderState, updateBuilderState] = useState<DataProductBuilderState>(DEFAULT_STATE);
    const [createDataProductMutation] = useCreateDataProductMutation();

    function createDataProduct() {
        createDataProductMutation({
            variables: {
                input: {
                    domainUrn: domain.urn,
                    id: builderState.id,
                    properties: {
                        name: builderState.name,
                        description: builderState.description || undefined,
                    },
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success(t('crud.success.createDataProduct'));
                    if (data?.createDataProduct) {
                        const updateDataProduct = { ...data.createDataProduct, domain: { domain } };
                        onCreateDataProduct(updateDataProduct as DataProduct);
                    }
                    onClose();
                }
            })
            .catch((error) => {
                onClose();
                message.destroy();
                message.error({ content: `${t('crud.error.failedCreateDataProduct')} ${error.message}.` });
            });
    }

    return (
        <Modal
            title={t('onBoarding.domains.createNewDataProduct')}
            onCancel={onClose}
            style={MODAL_BODY_STYLE}
            width={MODAL_WIDTH}
            open
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        {t('common.cancel')}
                    </Button>
                    <Button onClick={createDataProduct} disabled={!builderState.name}>
                        {t('common.create')}
                    </Button>
                </>
            }
        >
            <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
        </Modal>
    );
}
