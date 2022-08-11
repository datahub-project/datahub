import React from 'react';
import { Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { Message } from '../shared/Message';
import {
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    MatchedField,
    SearchAcrossEntitiesInput,
} from '../../types.generated';
import { SearchFilters } from './SearchFilters';
import { SearchCfg } from '../../conf';
import { SearchResultsRecommendations } from './SearchResultsRecommendations';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { SearchResultsInterface } from '../entity/shared/components/styled/search/types';
import SearchExtendedMenu from '../entity/shared/components/styled/search/SearchExtendedMenu';
import { combineSiblingsInSearchResults } from '../entity/shared/siblingUtils';
import { SearchSelectBar } from '../entity/shared/components/styled/search/SearchSelectBar';
import { SearchResultList } from './SearchResultList';
import { isListSubset } from '../entity/shared/utils';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { EntityAndType } from '../entity/shared/types';

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
    padding-left: 32px;
    padding-right: 32px;
    height: 47px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
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

const SearchResultsRecommendationsContainer = styled.div`
    margin-top: 40px;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding-left: 32px;
    padding-right: 32px;
`;

const SearchMenuContainer = styled.div``;

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
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    setIsSelectMode: (showSelectMode: boolean) => any;
    onChangeSelectAll: (selected: boolean) => void;
    refetch: () => void;
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
    isSelectMode,
    selectedEntities,
    setIsSelectMode,
    setSelectedEntities,
    onChangeSelectAll,
    refetch,
}: Props) => {
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;
    const authenticatedUserUrn = useGetAuthenticatedUser()?.corpUser?.urn;
    const combinedSiblingSearchResults = combineSiblingsInSearchResults(searchResponse?.searchResults);

    const searchResultUrns = combinedSiblingSearchResults.map((result) => result.entity.urn) || [];
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

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
                                onFilterSelect={(newFilters) => onChangeFilters(newFilters)}
                            />
                        </SearchFilterContainer>
                    </FiltersContainer>
                    <ResultContainer>
                        <PaginationInfoContainer>
                            <>
                                <Typography.Text>
                                    Showing{' '}
                                    <b>
                                        {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                                    </b>{' '}
                                    of <b>{totalResults}</b> results
                                </Typography.Text>
                                <SearchMenuContainer>
                                    <SearchExtendedMenu
                                        callSearchOnVariables={callSearchOnVariables}
                                        entityFilters={entityFilters}
                                        filters={filtersWithoutEntities}
                                        query={query}
                                        setShowSelectMode={setIsSelectMode}
                                    />
                                </SearchMenuContainer>
                            </>
                        </PaginationInfoContainer>
                        {isSelectMode && (
                            <StyledTabToolbar>
                                <SearchSelectBar
                                    isSelectAll={
                                        selectedEntities.length > 0 &&
                                        isListSubset(searchResultUrns, selectedEntityUrns)
                                    }
                                    selectedEntities={selectedEntities}
                                    onChangeSelectAll={onChangeSelectAll}
                                    onCancel={() => setIsSelectMode(false)}
                                    refetch={refetch}
                                />
                            </StyledTabToolbar>
                        )}
                        {!loading && (
                            <>
                                <SearchResultList
                                    query={query}
                                    searchResults={combinedSiblingSearchResults}
                                    totalResultCount={totalResults}
                                    isSelectMode={isSelectMode}
                                    selectedEntities={selectedEntities}
                                    setSelectedEntities={setSelectedEntities}
                                />
                                <PaginationControlContainer>
                                    <Pagination
                                        current={page}
                                        pageSize={numResultsPerPage}
                                        total={totalResults}
                                        showLessItems
                                        onChange={onChangePage}
                                        showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                                        onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage(newNum)}
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
