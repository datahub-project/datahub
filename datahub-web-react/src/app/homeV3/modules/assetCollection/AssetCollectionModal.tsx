import { Modal } from '@components';
import { Form } from 'antd';
import React from 'react';

import ModuleDetailsForm from '@app/homeV3/modules/assetCollection/ModuleDetailsForm';
import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';

interface Props {
    setShowAddAssetCollectionModal: React.Dispatch<React.SetStateAction<boolean>>;
}

const AssetCollectionModal = ({ setShowAddAssetCollectionModal }: Props) => {
    const [form] = Form.useForm();

    const handleModalClose = () => {
        setShowAddAssetCollectionModal(false);
    };

    const handleCreateAssetCollectionModule = () => {
        setShowAddAssetCollectionModal(false);
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
            <SelectAssetsSection />
        </Modal>
    );
};

export default AssetCollectionModal;
