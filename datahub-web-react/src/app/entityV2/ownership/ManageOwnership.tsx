import React from 'react';
import styled from 'styled-components/macro';
import { PageTitle } from '@components';
import { OwnershipList } from './OwnershipList';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
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
    return (
        <PageContainer>
            <PageTitle title="Manage Ownership" subTitle="Create, edit, and remove custom Ownership Types." />
            <ListContainer>
                <OwnershipList />
            </ListContainer>
        </PageContainer>
    );
};
