import React from 'react';
import { Button, Divider, Empty, List, ListProps, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { RocketOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';
import { Message } from '../shared/Message';
import {
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    MatchedField,
    SearchAcrossEntitiesInput,
    SearchResult,
} from '../../types.generated';
import { SearchFilters } from './SearchFilters';
import { useEntityRegistry } from '../useEntityRegistry';
import analytics from '../analytics/analytics';
import { EventType } from '../analytics';
import { SearchCfg } from '../../conf';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchResultsRecommendations } from './SearchResultsRecommendations';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { SearchResultsInterface } from '../entity/shared/components/styled/search/types';
import SearchExtendedMenu from '../entity/shared/components/styled/search/SearchExtendedMenu';
import {
    CombinedSearchResult,
    combineSiblingsInSearchResults,
    SEPARATE_SIBLINGS_URL_PARAM,
} from '../entity/shared/siblingUtils';
import { CompactEntityNameList } from '../recommendations/renderer/component/CompactEntityNameList';

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
    min-height: calc(100vh - 60px);
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
    margin-bottom: 20px;
    max-width: calc(100% - 260px);
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
    display: flex;
    justify-content: space-between;
`;

const FiltersHeader = styled.div`
    font-size: 14px;
    font-weight: 600;

    padding-left: 20px;
    padding-right: 20px;
    padding-bottom: 8px;

    width: 100%;
    height: 47px;
    line-height: 47px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const SearchFilterContainer = styled.div`
    padding-top: 10px;
`;

const NoDataContainer = styled.div`
    > div {
        margin-top: 28px;
        margin-bottom: 28px;
    }
`;

const ThinDivider = styled(Divider)`
    margin-top: 16px;
    margin-bottom: 16px;
`;

const SearchResultsRecommendationsContainer = styled.div`
    margin-top: 40px;
`;

const SearchMenuContainer = styled.div`
    margin-right: 10px;
`;

const SiblingResultContainer = styled.div`
    margin-top: 6px;
`;

interface Props {
    query: string;
    page: number;
    searchResponse?: {
        start: number;
        count: number;
        total: number;
        searchResults?: {
            entity: Entity;
            matchedFields: MatchedField[];
        }[];
    } | null;
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    loading: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filtersWithoutEntities: FacetFilterInput[];
    numResultsPerPage: number;
    setNumResultsPerPage: (numResults: number) => void;
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
    callSearchOnVariables,
    entityFilters,
    filtersWithoutEntities,
    numResultsPerPage,
    setNumResultsPerPage,
}: Props) => {
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;

    const entityRegistry = useEntityRegistry();
    const authenticatedUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;

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

    const onFilterSelect = (newFilters) => {
        onChangeFilters(newFilters);
    };

    const updateNumResults = (_currentNum: number, newNum: number) => {
        setNumResultsPerPage(newNum);
    };

    const history = useHistory();

    const combinedSiblingSearchResults = combineSiblingsInSearchResults(searchResponse?.searchResults);

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <div>
                <SearchBody>
                    <FiltersContainer>
                        <FiltersHeader>Filter</FiltersHeader>
                        <SearchFilterContainer>
                            <SearchFilters
                                loading={loading}
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
                            <SearchMenuContainer>
                                <SearchExtendedMenu
                                    callSearchOnVariables={callSearchOnVariables}
                                    entityFilters={entityFilters}
                                    filters={filtersWithoutEntities}
                                    query={query}
                                />
                            </SearchMenuContainer>
                        </PaginationInfoContainer>
                        {!loading && (
                            <>
                                <ResultList<React.FC<ListProps<CombinedSearchResult>>>
                                    dataSource={combinedSiblingSearchResults}
                                    split={false}
                                    locale={{
                                        emptyText: (
                                            <NoDataContainer>
                                                <Empty
                                                    style={{ fontSize: 18, color: ANTD_GRAY[8] }}
                                                    description={`No results found for "${query}"`}
                                                />
                                                <Button
                                                    onClick={() =>
                                                        navigateToSearchUrl({ query: '*', page: 0, history })
                                                    }
                                                >
                                                    <RocketOutlined /> Explore your metadata
                                                </Button>
                                            </NoDataContainer>
                                        ),
                                    }}
                                    renderItem={(item, index) => (
                                        <>
                                            <List.Item
                                                style={{ padding: 0 }}
                                                onClick={() => onResultClick(item, index)}
                                                // class name for counting in test purposes only
                                                className="test-search-result"
                                            >
                                                {entityRegistry.renderSearchResult(item.entity.type, item)}
                                            </List.Item>
                                            {item.matchedEntities && item.matchedEntities.length > 0 && (
                                                <SiblingResultContainer className="test-search-result-sibling-section">
                                                    <CompactEntityNameList
                                                        linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                                                        entities={item.matchedEntities}
                                                    />
                                                </SiblingResultContainer>
                                            )}
                                            <ThinDivider />
                                        </>
                                    )}
                                />
                                <PaginationControlContainer>
                                    <Pagination
                                        current={page}
                                        pageSize={numResultsPerPage}
                                        total={totalResults}
                                        showLessItems
                                        onChange={onChangePage}
                                        showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                                        onShowSizeChange={updateNumResults}
                                        pageSizeOptions={['10', '20', '50']}
                                    />
                                </PaginationControlContainer>
                                {authenticatedUserUrn && (
                                    <SearchResultsRecommendationsContainer>
                                        <SearchResultsRecommendations
                                            userUrn={authenticatedUserUrn}
                                            query={query}
                                            filters={selectedFilters}
                                        />
                                    </SearchResultsRecommendationsContainer>
                                )}
                            </>
                        )}
                    </ResultContainer>
                </SearchBody>
            </div>
        </>
    );
};
