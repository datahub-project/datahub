/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
