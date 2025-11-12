import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { OwnershipList } from '@app/entityV2/ownership/OwnershipList';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
    gap: 16px;
`;

/**
 * Component used for displaying the 'Manage Ownership' experience.
 */
export const ManageOwnership = () => {
    return (
        <PageContainer>
            <PageTitle title="Manage Ownership" subTitle="Create, edit, and remove custom Ownership Types." />
            <OwnershipList />
        </PageContainer>
    );
};
