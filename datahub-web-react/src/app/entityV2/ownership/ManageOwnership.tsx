import { Button, PageTitle } from '@components';
import { Plus } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { OwnershipBuilderModal } from '@app/entityV2/ownership/OwnershipBuilderModal';
import { OwnershipList } from '@app/entityV2/ownership/OwnershipList';

import { OwnershipTypeEntity } from '@types';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

/**
 * Component used for displaying the 'Manage Ownership' experience.
 */
export const ManageOwnership = () => {
    const [showOwnershipBuilder, setShowOwnershipBuilder] = useState<boolean>(false);
    const [ownershipType, setOwnershipType] = useState<undefined | OwnershipTypeEntity>(undefined);

    const onClickCreateOwnershipType = () => {
        setShowOwnershipBuilder(true);
    };

    const onCloseModal = () => {
        setShowOwnershipBuilder(false);
        setOwnershipType(undefined);
    };

    return (
        <PageContainer>
            <TitleContainer>
                <PageTitle title="Manage Ownership" subTitle="Create, edit, and remove custom Ownership Types." />
                <Button variant="filled" onClick={onClickCreateOwnershipType} data-testid="create-owner-type-v2">
                    <Plus size={16} /> Create
                </Button>
            </TitleContainer>
            <ListContainer>
                <OwnershipList />
            </ListContainer>
            <OwnershipBuilderModal
                isOpen={showOwnershipBuilder}
                onClose={onCloseModal}
                refetch={() => {}}
                ownershipType={ownershipType}
            />
        </PageContainer>
    );
};
