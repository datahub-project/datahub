import React, { useState } from 'react';
import styled from 'styled-components';
import { FilterSet } from '../../../entity/shared/components/styled/search/types';
import { EntityType, FacetFilterInput } from '../../../../types.generated';
import { UnionType } from '../../../search/utils/constants';
import { EmbeddedListSearch } from '../../../entity/shared/components/styled/search/EmbeddedListSearch';

const SearchContainer = styled.div`
    height: 40vh;
    max-height: 800px;
`;

type Props = {
    emptySearchQuery?: string | null;
    fixedFilters?: FilterSet;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    onTotalChanged?: (newTotal: number) => void;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
};

export const SearchSection = ({
    emptySearchQuery,
    fixedFilters,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    onTotalChanged,
    searchBarStyle,
    searchBarInputStyle,
}: Props) => {
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState(1);
    const [unionType, setUnionType] = useState(UnionType.AND);

    const [filters, setFilters] = useState<Array<FacetFilterInput>>([]);

    const onChangeQuery = (q: string) => {
        setQuery(q);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        setFilters(newFilters);
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    return (
        <SearchContainer>
            <EmbeddedListSearch
                entityTypes={[EntityType.Dataset]}
                query={query}
                filters={filters}
                page={page}
                unionType={unionType}
                onChangeQuery={onChangeQuery}
                onChangeFilters={onChangeFilters}
                onChangePage={onChangePage}
                onChangeUnionType={setUnionType}
                onTotalChanged={onTotalChanged}
                emptySearchQuery={emptySearchQuery}
                fixedFilters={fixedFilters}
                placeholderText={placeholderText}
                defaultShowFilters={defaultShowFilters}
                defaultFilters={defaultFilters}
                searchBarStyle={searchBarStyle}
                searchBarInputStyle={searchBarInputStyle}
                skipCache
            />
        </SearchContainer>
    );
};
