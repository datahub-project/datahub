import React from 'react';
import { Divider, List, ListProps, Pagination, Typography } from 'antd';
import styled from 'styled-components';

import { Message } from '../shared/Message';
import {
    FacetFilterInput,
    FacetMetadata,
    SearchResult,
    SearchResults as SearchResultType,
} from '../../types.generated';
import { SearchFilters } from './SearchFilters';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics from '../analytics/analytics';
import { EventType } from '../analytics';
import { SearchCfg } from '../../conf';

const ResultList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 8px;
        padding: 16px 32px;
        border-radius: 0px;
    }
`;

const SearchBody = styled.div`
    display: flex;
    flex-direction: row;
`;

const FiltersContainer = styled.div`
    display: block;
    max-width: 260px;
    min-width: 260px;
    border-right: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const ResultContainer = styled.div`
    flex: 1;
`;

const PaginationControlContainer = styled.div`
    padding-top: 16px;
    padding-bottom: 16px;
    text-align: center;
`;

const PaginationInfoContainer = styled.div`
    margin-top: 15px;
    padding-left: 16px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const FiltersHeader = styled.div`
    font-size: 14px;
    font-weight: 600;

    padding-left: 20px;
    padding-right: 20px;
    padding-bottom: 8px;

    width: 100%;
    height: 46px;
    line-height: 46px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const SearchFilterContainer = styled.div`
    padding-top: 10px;
`;

interface Props {
    query: string;
    page: number;
    searchResponse?: SearchResultType | null;
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    loading: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
}

export const SearchResults = ({
    query,
    page,
    searchResponse,
    filters,
    selectedFilters,
    loading,
    onChangeFilters,
    onChangePage,
}: Props) => {
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;

    const entityRegistry = useEntityRegistry();

    const onResultClick = (result: SearchResult, index: number) => {
        analytics.event({
            type: EventType.SearchResultClickEvent,
            query,
            entityUrn: result.entity.urn,
            entityType: result.entity.type,
            index,
            total: totalResults,
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
                    <FiltersHeader>Filter</FiltersHeader>
                    <SearchFilterContainer>
                        <SearchFilters
                            facets={filters || []}
                            selectedFilters={selectedFilters}
                            onFilterSelect={onFilterSelect}
                        />
                    </SearchFilterContainer>
                </FiltersContainer>
                <ResultContainer>
                    <PaginationInfoContainer>
                        <Typography.Paragraph>
                            Showing{' '}
                            <b>
                                {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                            </b>{' '}
                            of <b>{totalResults}</b> results
                        </Typography.Paragraph>
                    </PaginationInfoContainer>
                    <ResultList<React.FC<ListProps<SearchResult>>>
                        dataSource={searchResponse?.searchResults}
                        split={false}
                        renderItem={(item, index) => (
                            <>
                                <List.Item onClick={() => onResultClick(item, index)}>
                                    {entityRegistry.renderSearchResult(item.entity.type, item)}
                                </List.Item>
                                <Divider />
                            </>
                        )}
                    />
                    <PaginationControlContainer>
                        <Pagination
                            current={page}
                            pageSize={SearchCfg.RESULTS_PER_PAGE}
                            total={totalResults}
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </PaginationControlContainer>
                </ResultContainer>
            </SearchBody>
        </>
    );
};
