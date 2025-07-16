import { Form } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseCreateModuleModal from '@app/homeV3/createModule/modals/BaseCreateModuleModal';
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
        createModule,
        createModuleModalState: { position, close, isEditing, initialState },
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
            createModule({
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
        <BaseCreateModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Asset Collection`}
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            onUpsert={handleUpsertAssetCollectionModule}
        >
            <ModalContent>
                <ModuleDetailsForm form={form} formValues={{ name: currentName }} />
                <AssetsSection selectedAssetUrns={selectedAssetUrns} setSelectedAssetUrns={setSelectedAssetUrns} />
            </ModalContent>
        </BaseCreateModuleModal>
    );
};

export default AssetCollectionModal;
