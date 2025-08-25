import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import LinkItem from '@app/entityV2/summary/links/LinkItem';
import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { InstitutionalMemoryMetadata } from '@types';

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export default function LinksList() {
    const { entityData } = useEntityData();
    const links = entityData?.institutionalMemory?.elements || [];
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [selectedLink, setSelectedLink] = useState<InstitutionalMemoryMetadata | null>(null);
    const { handleDeleteLink } = useLinkUtils();

    if (links.length === 0) {
        return null;
    }

    const handleDelete = () => {
        if (selectedLink) {
            handleDeleteLink(selectedLink).then(() => {
                setSelectedLink(null);
                setShowConfirmDelete(false);
            });
        }
    };

    const handleCloseModal = () => {
        setShowConfirmDelete(false);
        setSelectedLink(null);
    };

    return (
        <>
            <ListContainer>
                {links.map((link) => {
                    return (
                        <LinkItem
                            link={link}
                            setSelectedLink={setSelectedLink}
                            setShowConfirmDelete={setShowConfirmDelete}
                        />
                    );
                })}
            </ListContainer>
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleCloseModal}
                handleConfirm={handleDelete}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete this link?"
            />
        </>
    );
}
