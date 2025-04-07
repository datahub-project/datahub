import React from 'react';
import { PageTitle } from '@components';
import styled from 'styled-components';
import { ViewsList } from './ViewsList';

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
 * Component used for displaying the 'Manage Views' experience.
 */
export const ManageViews = () => {
    return (
        <PageContainer>
            <PageTitle
                title="Manage Views"
                subTitle="Create, edit, and remove your Views. Views allow you to save and share sets of filters for reuse when browsing DataHub."
            />
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
