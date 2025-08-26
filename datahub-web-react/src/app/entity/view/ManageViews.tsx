import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ViewsList } from '@app/entity/view/ViewsList';

const PageContainer = styled.div`
    padding-top: 16px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 20px;
    }
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
            <PageHeaderContainer>
                <PageTitle
                    title="Manage Views"
                    subTitle="Create, edit, and remove your Views. Views allow you to save and share sets of filters for reuse when browsing DataHub."
                />
            </PageHeaderContainer>
            <ListContainer>
                <ViewsList />
            </ListContainer>
        </PageContainer>
    );
};
