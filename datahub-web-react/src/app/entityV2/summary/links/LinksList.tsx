import { useForm } from 'antd/lib/form/Form';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import AddEditLinkModal from '@app/entityV2/summary/links/AddEditLinkModal';
import LinkItem from '@app/entityV2/summary/links/LinkItem';
import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { InstitutionalMemoryMetadata } from '@types';

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 16px;
`;

export default function LinksList() {
    const { entityData } = useEntityData();
    const links = entityData?.institutionalMemory?.elements || [];
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [showEditLinkModal, setShowEditLinkModal] = useState(false);
    const [selectedLink, setSelectedLink] = useState<InstitutionalMemoryMetadata | null>(null);

    const { handleDeleteLink, handleUpdateLink, showInAssetPreview, setShowInAssetPreview } =
        useLinkUtils(selectedLink);
    const [form] = useForm();

    useEffect(() => {
        if (showEditLinkModal) {
            form.resetFields();
        }
    }, [showEditLinkModal, form]);

    if (links.length === 0) {
        return null;
    }

    const handleDelete = () => {
        if (selectedLink) {
            handleDeleteLink().then(() => {
                setSelectedLink(null);
                setShowConfirmDelete(false);
            });
        }
    };

    const handleCancelDelete = () => {
        setShowConfirmDelete(false);
        setSelectedLink(null);
    };

    const handleCloseUpdate = () => {
        setShowEditLinkModal(false);
        setSelectedLink(null);
        form.resetFields();
    };

    const handleUpdate = () => {
        if (selectedLink) {
            form.validateFields()
                .then((values) => handleUpdateLink(values))
                .then(() => handleCloseUpdate());
        }
    };

    if (!links.length) return null;

    return (
        <>
            <ListContainer>
                {links.map((link) => {
                    return (
                        <LinkItem
                            link={link}
                            setSelectedLink={setSelectedLink}
                            setShowConfirmDelete={setShowConfirmDelete}
                            setShowEditLinkModal={setShowEditLinkModal}
                        />
                    );
                })}
            </ListContainer>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleCancelDelete}
                handleConfirm={handleDelete}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete this link?"
                confirmButtonText="Delete"
                isDeleteModal
            />
            {showEditLinkModal && (
                <AddEditLinkModal
                    variant="update"
                    form={form}
                    initialValues={{
                        url: selectedLink?.url,
                        label: selectedLink?.label || selectedLink?.description,
                        showInAssetPreview: selectedLink?.settings?.showInAssetPreview,
                    }}
                    onClose={handleCloseUpdate}
                    onSubmit={handleUpdate}
                    showInAssetPreview={showInAssetPreview}
                    setShowInAssetPreview={setShowInAssetPreview}
                />
            )}
        </>
    );
}
