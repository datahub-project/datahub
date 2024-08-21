import React from 'react';
import { Pagination, Typography } from 'antd';
import styled from 'styled-components/macro';
import { Entity, FacetFilterInput, FacetMetadata, MatchedField, SearchSuggestion } from '../../types.generated';
import { SearchCfg } from '../../conf';
import { SearchResultsRecommendations } from './SearchResultsRecommendations';
import SearchExtendedMenu from '../entity/shared/components/styled/search/SearchExtendedMenu';
import { SearchSelectBar } from '../entity/shared/components/styled/search/SearchSelectBar';
import { SearchResultList } from './SearchResultList';
import { isListSubset } from '../entity/shared/utils';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { EntityAndType } from '../entity/shared/types';
import { ErrorSection } from '../shared/error/ErrorSection';
import { UnionType } from './utils/constants';
import { SearchFiltersSection } from './SearchFiltersSection';
import { generateOrFilters } from './utils/generateOrFilters';
import { SEARCH_RESULTS_FILTERS_ID } from '../onboarding/config/SearchOnboardingConfig';
import { useUserContext } from '../context/useUserContext';
import { DownloadSearchResults, DownloadSearchResultsInput } from './utils/types';
import BrowseSidebar from './sidebar';
import ToggleSidebarButton from './ToggleSidebarButton';
import { SidebarProvider } from './sidebar/SidebarContext';
import { BrowseProvider } from './sidebar/BrowseContext';
import { useIsBrowseV2, useIsSearchV2 } from './useSearchAndBrowseVersion';
import useToggleSidebar from './useToggleSidebar';
import SearchSortSelect from './sorting/SearchSortSelect';
import { combineSiblingsInSearchResults } from './utils/combineSiblingsInSearchResults';
import SearchQuerySuggester from './suggestions/SearchQuerySugggester';
import { ANTD_GRAY_V2 } from '../entity/shared/constants';
import { formatNumberWithoutAbbreviation } from '../shared/formatNumber';
import SearchResultsLoadingSection from './SearchResultsLoadingSection';
import { useIsShowSeparateSiblingsEnabled } from '../useAppConfig';

const SearchResultsWrapper = styled.div<{ v2Styles: boolean }>`
    display: flex;
    flex: 1;

    ${(props) =>
        props.v2Styles &&
        `
        overflow: hidden;
    `}
`;

const SearchBody = styled.div`
    display: flex;
    flex-direction: row;
    min-height: 100%;
    flex: 1;
    overflow: auto;
`;

const ResultContainer = styled.div<{ v2Styles: boolean }>`
    flex: 1;
    overflow: auto;
    ${(props) =>
        props.v2Styles
            ? `
        display: flex;
        flex-direction: column;
        background-color: ${ANTD_GRAY_V2[1]};
    `
            : `
        max-width: calc(100% - 260px);
    `}
`;

const PaginationControlContainer = styled.div`
    padding-top: 16px;
    padding-bottom: 16px;
    text-align: center;
`;

const PaginationInfoContainer = styled.div<{ v2Styles: boolean }>`
    padding-left: 24px;
    padding-right: 32px;
    height: 47px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
    ${({ v2Styles }) => v2Styles && `background-color: white;`}
`;

const LeftControlsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const SearchResultsRecommendationsContainer = styled.div`
    margin-top: 40px;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding-left: 32px;
    padding-right: 32px;
`;

const SearchMenuContainer = styled.div``;

const SearchResultListContainer = styled.div<{ v2Styles: boolean }>`
    ${({ v2Styles }) =>
        v2Styles &&
        `
        flex: 1;
        overflow: auto;
    `}
`;

interface Props {
    loading: boolean;
    unionType?: UnionType;
    query: string;
    viewUrn?: string;
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
    facets?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    error: any;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onChangePage: (page: number) => void;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
    numResultsPerPage: number;
    setNumResultsPerPage: (numResults: number) => void;
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    suggestions: SearchSuggestion[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    setIsSelectMode: (showSelectMode: boolean) => any;
    onChangeSelectAll: (selected: boolean) => void;
    refetch: () => void;
}

export const SearchResults = ({
    loading,
    unionType = UnionType.AND,
    query,
    viewUrn,
    page,
    searchResponse,
    facets,
    selectedFilters,
    error,
    onChangeUnionType,
    onChangeFilters,
    onChangePage,
    downloadSearchResults,
    numResultsPerPage,
    setNumResultsPerPage,
    isSelectMode,
    selectedEntities,
    suggestions,
    setIsSelectMode,
    setSelectedEntities,
    onChangeSelectAll,
    refetch,
}: Props) => {
    const showSearchFiltersV2 = useIsSearchV2();
    const showBrowseV2 = useIsBrowseV2();
    const { isSidebarOpen, toggleSidebar } = useToggleSidebar();
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;
    const authenticatedUserUrn = useUserContext().user?.urn;
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const combinedSiblingSearchResults = combineSiblingsInSearchResults(
        showSeparateSiblings,
        searchResponse?.searchResults,
    );

    const searchResultUrns = combinedSiblingSearchResults.map((result) => result.entity.urn) || [];
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

    return (
        <>
            <SearchResultsWrapper v2Styles={showSearchFiltersV2}>
                <SearchBody>
                    {!showSearchFiltersV2 && (
                        <div id={SEARCH_RESULTS_FILTERS_ID} data-testid="search-filters-v1">
                            <SearchFiltersSection
                                filters={facets}
                                selectedFilters={selectedFilters}
                                unionType={unionType}
                                loading={loading}
                                onChangeFilters={onChangeFilters}
                                onChangeUnionType={onChangeUnionType}
                            />
                        </div>
                    )}
                    {showBrowseV2 && (
                        <SidebarProvider selectedFilters={selectedFilters} onChangeFilters={onChangeFilters}>
                            <BrowseProvider>
                                <BrowseSidebar visible={isSidebarOpen} />
                            </BrowseProvider>
                        </SidebarProvider>
                    )}
                    <ResultContainer v2Styles={showSearchFiltersV2}>
                        <PaginationInfoContainer v2Styles={showSearchFiltersV2}>
                            <LeftControlsContainer>
                                {showBrowseV2 && <ToggleSidebarButton isOpen={isSidebarOpen} onClick={toggleSidebar} />}
                                <Typography.Text>
                                    Showing{' '}
                                    <b>
                                        {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                                    </b>{' '}
                                    of{' '}
                                    <b>
                                        {totalResults >= 10000
                                            ? `${formatNumberWithoutAbbreviation(10000)}+`
                                            : formatNumberWithoutAbbreviation(totalResults)}
                                    </b>{' '}
                                    results
                                </Typography.Text>
                            </LeftControlsContainer>
                            <SearchMenuContainer>
                                <SearchSortSelect />
                                <SearchExtendedMenu
                                    downloadSearchResults={downloadSearchResults}
                                    filters={generateOrFilters(unionType, selectedFilters)}
                                    query={query}
                                    viewUrn={viewUrn}
                                    setShowSelectMode={setIsSelectMode}
                                    totalResults={totalResults}
                                />
                            </SearchMenuContainer>
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
                        {(error && <ErrorSection />) ||
                            (loading && !combinedSiblingSearchResults.length && <SearchResultsLoadingSection />) ||
                            (combinedSiblingSearchResults && (
                                <SearchResultListContainer v2Styles={showSearchFiltersV2}>
                                    {totalResults > 0 && <SearchQuerySuggester suggestions={suggestions} />}
                                    <SearchResultList
                                        loading={loading}
                                        query={query}
                                        searchResults={combinedSiblingSearchResults}
                                        totalResultCount={totalResults}
                                        isSelectMode={isSelectMode}
                                        selectedEntities={selectedEntities}
                                        setSelectedEntities={setSelectedEntities}
                                        suggestions={suggestions}
                                        pageNumber={page}
                                    />
                                    {totalResults > 0 && (
                                        <PaginationControlContainer id="search-pagination">
                                            <Pagination
                                                current={page}
                                                pageSize={numResultsPerPage}
                                                total={totalResults}
                                                showLessItems
                                                onChange={onChangePage}
                                                showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                                                onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage(newNum)}
                                                pageSizeOptions={['10', '20', '50', '100']}
                                            />
                                        </PaginationControlContainer>
                                    )}
                                    {authenticatedUserUrn && (
                                        <SearchResultsRecommendationsContainer>
                                            <SearchResultsRecommendations
                                                userUrn={authenticatedUserUrn}
                                                query={query}
                                                filters={selectedFilters}
                                            />
                                        </SearchResultsRecommendationsContainer>
                                    )}
                                </SearchResultListContainer>
                            ))}
                    </ResultContainer>
                </SearchBody>
            </SearchResultsWrapper>
        </>
    );
};
