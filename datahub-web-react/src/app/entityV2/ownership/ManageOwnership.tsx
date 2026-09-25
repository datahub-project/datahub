import { Button, PageTitle } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('entity.ownership');
    const [showOwnershipBuilder, setShowOwnershipBuilder] = useState(false);

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle title={t('pageTitle')} subTitle={t('pageSubTitle')} />
                <HeaderRight>
                    <Button
                        variant="filled"
                        onClick={() => setShowOwnershipBuilder(true)}
                        data-testid="create-owner-type-v2"
                    >
                        {t('createOwnershipType')}
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
