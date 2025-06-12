import { colors } from '@components';
import { Pagination } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { PreviewType } from '@app/entity/Entity';
import { EntityAndType } from '@app/entity/shared/types';
import { isListSubset } from '@app/entity/shared/utils';
import { SearchSelectBar } from '@app/entityV2/shared/components/styled/search/SearchSelectBar';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { SearchEntitySidebarContainer } from '@app/searchV2/SearchEntitySidebarContainer';
import { SearchResultList } from '@app/searchV2/SearchResultList';
import SearchResultsLoadingSection from '@app/searchV2/SearchResultsLoadingSection';
import BrowseSidebar from '@app/searchV2/sidebar';
import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import { SidebarProvider } from '@app/searchV2/sidebar/SidebarContext';
import SearchQuerySuggester from '@app/searchV2/suggestions/SearchQuerySugggester';
import { useIsBrowseV2, useIsSearchV2 } from '@app/searchV2/useSearchAndBrowseVersion';
import { combineSiblingsInSearchResults } from '@app/searchV2/utils/combineSiblingsInSearchResults';
import { ErrorSection } from '@app/shared/error/ErrorSection';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { useIsShowSeparateSiblingsEnabled } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { SearchCfg } from '@src/conf';

import { Entity, FacetFilterInput, MatchedField, SearchSuggestion } from '@types';

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
    `
            : `
        max-width: calc(100% - 260px);
    `}
`;

const PaginationControlContainer = styled.div`
    padding: 16px;
    text-align: start;
`;

const PaginationInfoContainer = styled.div<{ v2Styles: boolean }>`
    padding: 4px 8px 4px 12px;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SearchResultsContainer = styled.div`
    display: flex;
    height: 100%;
`;

const SearchResultsScrollContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex-direction: column;
    height: 100%;
    ${(props) => !props.$isShowNavBarRedesign && 'overflow-y: scroll;'}
`;

const LeftControlsContainer = styled.div`
    display: flex;
    color: ${colors.gray[1700]};
    gap: 4px;
`;

const StyledTabToolbar = styled.div`
    background-color: #fff;
    border-radius: 12px;
    margin: 4px 16px 4px 8px;
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border: 1.5px solid ${ANTD_GRAY[4]};
`;

const SearchResultListContainer = styled.div<{ v2Styles: boolean; $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex-direction: column;
    ${({ v2Styles, $isShowNavBarRedesign }) =>
        v2Styles &&
        `
        flex: 1;
        overflow-x: hidden;        
        overflow-y: auto;
        ${$isShowNavBarRedesign ? 'scrollbar-width: none;' : ''}
    `}
    margin: ${(props) => (props.$isShowNavBarRedesign ? '5px 4px 5px 0px' : '4px 12px 4px 0px')};
`;

interface Props {
    loading: boolean;
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
    selectedFilters: Array<FacetFilterInput>;
    error: any;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
    numResultsPerPage: number;
    setNumResultsPerPage: (numResults: number) => void;
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    suggestions: SearchSuggestion[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    areAllEntitiesSelected?: boolean;
    setAreAllEntitiesSelected?: (areAllSelected: boolean) => void;
    setIsSelectMode: (showSelectMode: boolean) => any;
    onChangeSelectAll: (selected: boolean) => void;
    refetch: () => void;
    previewType?: PreviewType;
    onCardClick?: (any: any) => any;
}

export const SearchResults = ({
    loading,
    query,
    page,
    searchResponse,
    selectedFilters,
    error,
    onChangeFilters,
    onChangePage,
    numResultsPerPage,
    setNumResultsPerPage,
    isSelectMode,
    selectedEntities,
    suggestions,
    setSelectedEntities,
    areAllEntitiesSelected,
    setAreAllEntitiesSelected,
    setIsSelectMode,
    onChangeSelectAll,
    refetch,
    previewType,
    onCardClick,
}: Props) => {
    const showSearchFiltersV2 = useIsSearchV2();
    const showBrowseV2 = useIsBrowseV2();
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const combinedSiblingSearchResults = combineSiblingsInSearchResults(
        showSeparateSiblings,
        searchResponse?.searchResults,
    );
    // For vertical sidebar
    const [highlightedIndex, setHighlightedIndex] = useState<number | null>(0);

    const searchResultUrns = combinedSiblingSearchResults.map((result) => result.entity.urn) || [];
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

    const [resultsHeight, setResultsHeight] = useState('calc(100vh - 155px)');
    const resultsRef = React.useCallback((node: HTMLDivElement) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setResultsHeight(`${node.offsetHeight}px`);
            });
            resizeObserver.observe(node);
        }
    }, []);

    function handlePageChange(newPage: number, newPageSize: number) {
        const didPageSizeChange = numResultsPerPage !== newPageSize;
        if (didPageSizeChange) {
            onChangePage(1);
            setNumResultsPerPage(newPageSize);
        } else {
            onChangePage(newPage);
        }
        setAreAllEntitiesSelected?.(false);
    }

    return (
        <>
            <SearchResultsWrapper v2Styles={showSearchFiltersV2}>
                <SearchBody>
                    {showBrowseV2 && (
                        <SidebarProvider selectedFilters={selectedFilters} onChangeFilters={onChangeFilters}>
                            <BrowseProvider>
                                <BrowseSidebar visible />
                            </BrowseProvider>
                        </SidebarProvider>
                    )}
                    <ResultContainer v2Styles={showSearchFiltersV2} ref={resultsRef}>
                        {(error && <ErrorSection />) ||
                            (loading && !combinedSiblingSearchResults.length && <SearchResultsLoadingSection />) ||
                            (combinedSiblingSearchResults && (
                                <SearchResultsScrollContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                                    <SearchResultsContainer>
                                        <SearchResultListContainer
                                            v2Styles={showSearchFiltersV2}
                                            $isShowNavBarRedesign={isShowNavBarRedesign}
                                        >
                                            {totalResults > 0 && <SearchQuerySuggester suggestions={suggestions} />}

                                            {isSelectMode && (
                                                <StyledTabToolbar>
                                                    <SearchSelectBar
                                                        isSelectAll={
                                                            selectedEntities.length > 0 &&
                                                            isListSubset(searchResultUrns, selectedEntityUrns)
                                                        }
                                                        totalResults={totalResults}
                                                        selectedEntities={selectedEntities}
                                                        setSelectedEntities={setSelectedEntities}
                                                        onChangeSelectAll={onChangeSelectAll}
                                                        onCancel={() => setIsSelectMode(false)}
                                                        refetch={refetch}
                                                        areAllEntitiesSelected={areAllEntitiesSelected}
                                                        setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                                                    />
                                                </StyledTabToolbar>
                                            )}
                                            <PaginationInfoContainer v2Styles={showSearchFiltersV2}>
                                                <LeftControlsContainer>
                                                    Showing{' '}
                                                    <b>
                                                        {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} -{' '}
                                                        {lastResultIndex}
                                                    </b>{' '}
                                                    of{' '}
                                                    <b>
                                                        {totalResults >= 10000
                                                            ? `${formatNumberWithoutAbbreviation(10000)}+`
                                                            : formatNumberWithoutAbbreviation(totalResults)}
                                                    </b>{' '}
                                                    results
                                                </LeftControlsContainer>
                                            </PaginationInfoContainer>
                                            <SearchResultList
                                                setHighlightedIndex={setHighlightedIndex}
                                                highlightedIndex={highlightedIndex}
                                                loading={loading}
                                                query={query}
                                                searchResults={combinedSiblingSearchResults}
                                                totalResultCount={totalResults}
                                                isSelectMode={isSelectMode}
                                                selectedEntities={selectedEntities}
                                                setSelectedEntities={setSelectedEntities}
                                                suggestions={suggestions}
                                                pageNumber={page}
                                                previewType={previewType}
                                                onCardClick={onCardClick}
                                                setAreAllEntitiesSelected={setAreAllEntitiesSelected}
                                            />
                                            {totalResults > 0 && (
                                                <PaginationControlContainer id="search-pagination">
                                                    <Pagination
                                                        current={page}
                                                        pageSize={numResultsPerPage}
                                                        total={totalResults}
                                                        showLessItems
                                                        onChange={handlePageChange}
                                                        showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                                                        pageSizeOptions={['10', '20', '30']}
                                                    />
                                                </PaginationControlContainer>
                                            )}
                                        </SearchResultListContainer>
                                        <SearchEntitySidebarContainer
                                            height={resultsHeight}
                                            highlightedIndex={highlightedIndex}
                                            selectedEntity={
                                                highlightedIndex !== null &&
                                                combinedSiblingSearchResults?.length > highlightedIndex
                                                    ? {
                                                          urn: combinedSiblingSearchResults[highlightedIndex]?.entity
                                                              .urn,
                                                          type: combinedSiblingSearchResults[highlightedIndex]?.entity
                                                              .type,
                                                      }
                                                    : null
                                            }
                                        />
                                    </SearchResultsContainer>
                                </SearchResultsScrollContainer>
                            ))}
                    </ResultContainer>
                </SearchBody>
            </SearchResultsWrapper>
        </>
    );
};
