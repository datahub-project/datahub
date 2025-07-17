import { Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/modals/BaseModuleModal';
import AssetsSection from '@app/homeV3/modules/assetCollection/AssetsSection';
import ModuleDetailsForm from '@app/homeV3/modules/assetCollection/ModuleDetailsForm';

import { DataHubPageModuleType } from '@types';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const AssetCollectionModal = () => {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();
    const [form] = Form.useForm();
    const currentName = initialState?.properties.name || '';
    const currentAssets = (initialState?.properties?.params?.assetCollectionParams?.assetUrns || []).filter(
        (urn): urn is string => typeof urn === 'string',
    );
    const urn = initialState?.urn;
    const [selectedAssetUrns, setSelectedAssetUrns] = useState<string[]>(currentAssets);

    const handleUpsertAssetCollectionModule = () => {
        form.validateFields().then((values) => {
            const { name } = values;
            upsertModule({
                urn,
                name,
                position: position ?? {},
                type: DataHubPageModuleType.AssetCollection,
                params: {
                    assetCollectionParams: {
                        assetUrns: selectedAssetUrns,
                    },
                },
            });
            close();
        });
    };

    return (
        <BaseModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Asset Collection`}
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            onUpsert={handleUpsertAssetCollectionModule}
        >
            <ModalContent>
                <ModuleDetailsForm form={form} formValues={{ name: currentName }} />
                <AssetsSection selectedAssetUrns={selectedAssetUrns} setSelectedAssetUrns={setSelectedAssetUrns} />
            </ModalContent>
        </BaseModuleModal>
    );
};

export default AssetCollectionModal;
