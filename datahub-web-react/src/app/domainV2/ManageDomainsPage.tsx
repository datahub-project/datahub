import { Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { DomainsList } from './DomainsList';
import { DomainsContext } from './DomainsContext';
import { GenericEntityProperties } from '../entity/shared/types';

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

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData }}>
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
