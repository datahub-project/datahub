import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import useGetUserGroupUrns from '@app/entityV2/user/useGetUserGroupUrns';
import { UnionType } from '@app/search/utils/constants';

const UserAssetsWrapper = styled.div`
    height: 100%;
    overflow: auto;
`;

type Props = {
    urn: string;
};

export const UserAssets = ({ urn }: Props) => {
    const { t } = useTranslation('entity.types');
    const { groupUrns, data, loading } = useGetUserGroupUrns(urn);

    if (!data || loading) return null;

    return (
        <UserAssetsWrapper>
            <EmbeddedListSearchSection
                skipCache
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [{ field: 'owners', values: [urn, ...groupUrns] }],
                }}
                emptySearchQuery="*"
                placeholderText={t('shared.filterEntitiesPlaceholder')}
            />
        </UserAssetsWrapper>
    );
};
