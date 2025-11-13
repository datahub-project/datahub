import { useForm } from 'antd/lib/form/Form';
import React from 'react';
import { LinkFormData } from '@app/entityV2/shared/components/links/types';
import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';
import AddEditLinkModal from '@app/entityV2/shared/components/links/AddEditLinkModal';


type Props = {
    setShowAddLinkModal: React.Dispatch<React.SetStateAction<boolean>>;
};

export default function AddLinkModal({ setShowAddLinkModal }: Props) {
    const [form] = useForm<LinkFormData>();
    const { handleAddLink, showInAssetPreview, setShowInAssetPreview } = useLinkUtils();

    const handleClose = () => {
        setShowAddLinkModal(false);
    };

    const handleAdd = () => {
        form.validateFields()
            .then((values) => handleAddLink(values))
            .then(() => handleClose());
    };

    return (
        <AddEditLinkModal
            variant="create"
            form={form}
            onSubmit={handleAdd}
            onClose={handleClose}
            showInAssetPreview={showInAssetPreview}
            setShowInAssetPreview={setShowInAssetPreview}
        />
    );
}
