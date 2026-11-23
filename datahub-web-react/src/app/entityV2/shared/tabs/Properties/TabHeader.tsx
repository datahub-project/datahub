import { SearchBar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AddPropertyButton from '@app/entityV2/shared/tabs/Properties/AddPropertyButton';
import { Maybe, StructuredProperties } from '@src/types.generated';

// Matches the Columns tab toolbar: no divider, since the bordered table below draws its own edge.
const TableHeader = styled.div`
    padding: 8px 16px 16px 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
`;

interface Props {
    setFilterText: (text: string) => void;
    fieldUrn?: string;
    fieldProperties?: Maybe<StructuredProperties>;
    refetch?: () => void;
}

export default function TabHeader({ setFilterText, fieldUrn, fieldProperties, refetch }: Props) {
    const { t } = useTranslation('entity.profile.tabs');
    return (
        <TableHeader>
            <SearchBar
                placeholder={t('properties.searchInProperties.placeholder')}
                onChange={(value) => setFilterText(value)}
                width="300px"
            />
            <AddPropertyButton fieldUrn={fieldUrn} fieldProperties={fieldProperties} refetch={refetch} />
        </TableHeader>
    );
}
