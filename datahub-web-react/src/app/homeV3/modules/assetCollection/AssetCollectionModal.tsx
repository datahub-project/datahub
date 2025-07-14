import { Modal } from '@components';
import { Form } from 'antd';
import React, { useState } from 'react';

import ModuleDetailsForm from '@app/homeV3/modules/assetCollection/ModuleDetailsForm';
import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';

import { DataHubPageModuleType, PageModuleParamsInput } from '@types';

interface Props {
    setShowAddAssetCollectionModal: React.Dispatch<React.SetStateAction<boolean>>;
    handleCreateNewModule: (type: DataHubPageModuleType, name: string, params: PageModuleParamsInput) => void;
}

const AssetCollectionModal = ({ setShowAddAssetCollectionModal, handleCreateNewModule }: Props) => {
    const [form] = Form.useForm();
    const [selectedAssetUrns, setSelectedAssetUrns] = useState<string[]>([]);

    const handleModalClose = () => {
        setShowAddAssetCollectionModal(false);
    };

    const handleCreateAssetCollectionModule = () => {
        form.validateFields().then((values) => {
            const { name } = values;
            handleCreateNewModule(DataHubPageModuleType.AssetCollection, name, {
                assetCollectionParams: {
                    assetUrns: selectedAssetUrns,
                },
            });
            setShowAddAssetCollectionModal(false);
        });
    };

    return (
        <Modal
            title="Add Asset Collection"
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            buttons={[
                { text: 'Cancel', variant: 'outline', onClick: handleModalClose },
                { text: 'Create', variant: 'filled', onClick: handleCreateAssetCollectionModule },
            ]}
            onCancel={handleModalClose}
            width="800px"
        >
            <ModuleDetailsForm form={form} />
            <SelectAssetsSection selectedAssetUrns={selectedAssetUrns} setSelectedAssetUrns={setSelectedAssetUrns} />
        </Modal>
    );
};

export default AssetCollectionModal;
