import { useForm } from 'antd/lib/form/Form';
import React from 'react';

import AddEditLinkModal from '@app/entityV2/summary/links/AddEditLinkModal';
import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';

type Props = {
    setShowAddLinkModal: React.Dispatch<React.SetStateAction<boolean>>;
};

export default function AddLinkModal({ setShowAddLinkModal }: Props) {
    const [form] = useForm();
    const { handleAddLink } = useLinkUtils();

    const handleClose = () => {
        setShowAddLinkModal(false);
    };

    const handleAdd = () => {
        form.validateFields()
            .then((values) => handleAddLink(values))
            .then(() => handleClose());
    };

    return <AddEditLinkModal variant="create" form={form} onSubmit={handleAdd} onClose={handleClose} />;
}
