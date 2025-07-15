import { Form } from 'antd';
import React, { useState } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseCreateModuleModal from '@app/homeV3/createModule/modals/BaseCreateModuleModal';
import ModuleDetailsForm from '@app/homeV3/modules/assetCollection/ModuleDetailsForm';
import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';

import { DataHubPageModuleType } from '@types';

const AssetCollectionModal = () => {
    const {
        createModule,
        createModuleModalState: { position, close },
    } = usePageTemplateContext();
    const [form] = Form.useForm();
    const [selectedAssetUrns, setSelectedAssetUrns] = useState<string[]>([]);

    const handleCreateAssetCollectionModule = () => {
        form.validateFields().then((values) => {
            const { name } = values;
            createModule({
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
            title="Add Asset Collection"
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            onCreate={handleCreateAssetCollectionModule}
        >
            <ModuleDetailsForm form={form} />
            <SelectAssetsSection selectedAssetUrns={selectedAssetUrns} setSelectedAssetUrns={setSelectedAssetUrns} />
        </BaseCreateModuleModal>
    );
};

export default AssetCollectionModal;
