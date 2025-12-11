/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DomainsContext, UpdatedDomain } from '@app/domainV2/DomainsContext';
import { DomainsList } from '@app/domainV2/DomainsList';
import { GenericEntityProperties } from '@app/entity/shared/types';

const PageContainer = styled.div`
    padding-top: 20px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const ListContainer = styled.div``;

export const ManageDomainsPage = () => {
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [newDomain, setNewDomain] = useState<UpdatedDomain | null>(null);
    const [deletedDomain, setDeletedDomain] = useState<UpdatedDomain | null>(null);
    const [updatedDomain, setUpdatedDomain] = useState<UpdatedDomain | null>(null);

    return (
        <DomainsContext.Provider
            value={{
                entityData,
                setEntityData,
                newDomain,
                setNewDomain,
                deletedDomain,
                setDeletedDomain,
                updatedDomain,
                setUpdatedDomain,
            }}
        >
            <PageContainer>
                <PageHeaderContainer>
                    <PageTitle level={3}>Domains</PageTitle>
                    <Typography.Paragraph type="secondary">
                        View your DataHub Domains. Take administrative actions.
                    </Typography.Paragraph>
                </PageHeaderContainer>
                <ListContainer>
                    <DomainsList />
                </ListContainer>
            </PageContainer>
        </DomainsContext.Provider>
    );
};
