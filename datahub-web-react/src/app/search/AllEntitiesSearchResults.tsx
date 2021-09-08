import React from 'react';
import { Divider, List, ListProps } from 'antd';
import styled from 'styled-components';

import { Message } from '../shared/Message';
import { FacetFilterInput, FacetMetadata, SearchResult } from '../../types.generated';
import { SearchFilters } from './SearchFilters';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics from '../analytics/analytics';
import { EventType } from '../analytics';

const ResultList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 8px;
        padding: 16px 32px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

const SearchBody = styled.div`
    display: flex;
    flex-direction: row;
`;

const FiltersContainer = styled.div`
    margin-top: 10px;
    display: block;
    max-width: 260px;
    min-width: 260px;
`;

interface Props {
    query: string;
    searchResults?: SearchResult[];
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    loading: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
}

export const AllEntitiesSearchResults = ({
    query,
    searchResults,
    filters,
    selectedFilters,
    loading,
    onChangeFilters,
    onChangePage,
}: Props) => {
    console.log(onChangePage);
    const entityRegistry = useEntityRegistry();

    const onResultClick = (result: SearchResult, index: number) => {
        analytics.event({
            type: EventType.SearchResultClickEvent,
            query,
            entityUrn: result.entity.urn,
            entityType: result.entity.type,
            index,
            /// TODO(Gabe)
            total: 0,
        });
    };

    const onFilterSelect = (selected: boolean, field: string, value: string) => {
        const newFilters = selected
            ? [...selectedFilters, { field, value }]
            : selectedFilters.filter((filter) => filter.field !== field || filter.value !== value);
        onChangeFilters(newFilters);
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <SearchBody>
                <FiltersContainer>
                    <SearchFilters
                        facets={filters || []}
                        selectedFilters={selectedFilters}
                        onFilterSelect={onFilterSelect}
                    />
                </FiltersContainer>
                <ResultList<React.FC<ListProps<SearchResult>>>
                    dataSource={searchResults}
                    split={false}
                    renderItem={(item, index) => (
                        <>
                            <List.Item onClick={() => onResultClick(item, index)}>
                                {entityRegistry.renderSearchResult(item.entity.type, item)}
                            </List.Item>
                            <Divider />
                        </>
                    )}
                    bordered
                />
            </SearchBody>
        </>
    );
};
