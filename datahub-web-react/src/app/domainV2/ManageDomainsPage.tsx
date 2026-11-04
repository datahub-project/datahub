import { Heading, Text } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DomainsContext, UpdatedDomain } from '@app/domainV2/DomainsContext';
import { DomainsList } from '@app/domainV2/DomainsList';
import { GenericEntityProperties } from '@app/entityV2/shared/types';

const PageContainer = styled.div`
    padding-top: 20px;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled.div`
    margin-bottom: 12px;
`;

const ListContainer = styled.div``;

export const ManageDomainsPage = () => {
    const { t } = useTranslation('governance.domain');
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
                    <PageTitle>
                        <Heading type="h3" size="4xl" weight="semiBold">
                            {t('page.title')}
                        </Heading>
                    </PageTitle>
                    <Text color="textSecondary">{t('page.subtitle')}</Text>
                </PageHeaderContainer>
                <ListContainer>
                    <DomainsList />
                </ListContainer>
            </PageContainer>
        </DomainsContext.Provider>
    );
};
