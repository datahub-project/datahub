import { Button, PageTitle } from '@components';
import React, { useState } from 'react';
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

const PageHeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: center;
`;

/**
 * Component used for displaying the 'Manage Ownership' experience.
 */
export const ManageOwnership = () => {
    const [showOwnershipBuilder, setShowOwnershipBuilder] = useState(false);

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle title="Manage Ownership" subTitle="Create, edit, and remove custom Ownership Types." />
                <HeaderRight>
                    <Button
                        variant="filled"
                        onClick={() => setShowOwnershipBuilder(true)}
                        data-testid="create-owner-type-v2"
                    >
                        Create Ownership Type
                    </Button>
                </HeaderRight>
            </PageHeaderContainer>
            <OwnershipList
                showOwnershipBuilder={showOwnershipBuilder}
                setShowOwnershipBuilder={setShowOwnershipBuilder}
            />
        </PageContainer>
    );
};
