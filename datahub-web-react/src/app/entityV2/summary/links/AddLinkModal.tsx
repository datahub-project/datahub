/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useForm } from 'antd/lib/form/Form';
import React from 'react';

import AddEditLinkModal from '@app/entityV2/summary/links/AddEditLinkModal';
import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';

type Props = {
    setShowAddLinkModal: React.Dispatch<React.SetStateAction<boolean>>;
};

export default function AddLinkModal({ setShowAddLinkModal }: Props) {
    const [form] = useForm();
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
