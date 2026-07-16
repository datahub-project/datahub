import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EmbeddedListSearchSection } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection';
import { UnionType } from '@app/search/utils/constants';

const GroupAssetsWrapper = styled.div`
    height: 100%;
`;

type Props = {
    urn: string;
};

export const GroupAssets = ({ urn }: Props) => {
    const { t } = useTranslation('entity.types');
    return (
        <GroupAssetsWrapper>
            <EmbeddedListSearchSection
                skipCache
                fixedFilters={{
                    unionType: UnionType.AND,
                    filters: [{ field: 'owners', values: [urn] }],
                }}
                emptySearchQuery="*"
                placeholderText={t('shared.filterEntitiesPlaceholder')}
            />
        </GroupAssetsWrapper>
    );
};
