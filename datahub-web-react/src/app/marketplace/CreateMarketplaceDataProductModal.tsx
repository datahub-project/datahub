import { Modal, toast } from '@components';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DataProductBuilderForm from '@app/entityV2/domain/DataProductsTab/DataProductBuilderForm';
import { DataProductBuilderState } from '@app/entityV2/domain/DataProductsTab/types';
import DomainSelector from '@app/entityV2/shared/DomainSelector/DomainSelector';

import { useCreateDataProductMutation } from '@graphql/dataProduct.generated';
import { DataProduct } from '@types';

const DEFAULT_STATE: DataProductBuilderState = {
    name: '',
};

const FormFields = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

type Props = {
    open: boolean;
    onClose: () => void;
    onCreateDataProduct: (dataProduct: DataProduct) => void;
};

export default function CreateMarketplaceDataProductModal({ open, onClose, onCreateDataProduct }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const [selectedDomainUrns, setSelectedDomainUrns] = useState<string[]>([]);
    const [builderState, updateBuilderState] = useState<DataProductBuilderState>(DEFAULT_STATE);
    const [domainSelectorKey, setDomainSelectorKey] = useState(0);
    const [createDataProductMutation, { loading: isCreating }] = useCreateDataProductMutation();

    const domainUrn = selectedDomainUrns[0];

    useEffect(() => {
        if (open) {
            setDomainSelectorKey((key) => key + 1);
            return;
        }

        setSelectedDomainUrns([]);
        updateBuilderState(DEFAULT_STATE);
    }, [open]);

    function createDataProduct() {
        if (!domainUrn || isCreating) return;

        createDataProductMutation({
            variables: {
                input: {
                    domainUrn,
                    properties: {
                        name: builderState.name,
                        description: builderState.description || undefined,
                        parentDataProduct: builderState.parentDataProductUrn || undefined,
                    },
                },
            },
        })
            .then(({ data, errors }) => {
                if (!errors && data?.createDataProduct) {
                    toast.success(t('dataProduct.createSuccess'));
                    onCreateDataProduct(data.createDataProduct as DataProduct);
                    onClose();
                }
            })
            .catch(() => {
                toast.destroy();
                toast.error(t('dataProduct.createError'));
            });
    }

    return (
        <Modal
            title={t('dataProduct.createTitle')}
            onCancel={onClose}
            open={open}
            width={725}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                    disabled: isCreating,
                    buttonDataTestId: 'cancel-button',
                },
                {
                    text: tc('create'),
                    onClick: createDataProduct,
                    variant: 'filled',
                    disabled: !builderState.name || !domainUrn || isCreating,
                    isLoading: isCreating,
                    buttonDataTestId: 'submit-button',
                },
            ]}
            data-testid="create-marketplace-data-product-modal"
        >
            {open && (
                <FormFields>
                    <DomainSelector
                        key={domainSelectorKey}
                        selectedDomains={selectedDomainUrns}
                        onDomainsChange={(urns) => setSelectedDomainUrns(urns.length ? [urns[0]] : [])}
                        isMultiSelect={false}
                        isRequired
                    />
                    <DataProductBuilderForm builderState={builderState} updateBuilderState={updateBuilderState} />
                </FormFields>
            )}
        </Modal>
    );
}
