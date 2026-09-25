import { SearchBar } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AddPropertyButton from '@app/entity/shared/tabs/Properties/AddPropertyButton';
import { Maybe, StructuredProperties } from '@src/types.generated';

const TableHeader = styled.div`
    padding: 8px 16px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
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
