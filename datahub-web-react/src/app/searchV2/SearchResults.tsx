import React, { useState } from 'react';
import { Pagination, Typography } from 'antd';
import styled from 'styled-components/macro';
import { Entity, FacetFilterInput, MatchedField, SearchSuggestion, FacetMetadata } from '../../types.generated';
import { SearchCfg } from '../../conf';
import { SearchSelectBar } from '../entityV2/shared/components/styled/search/SearchSelectBar';
import { SearchResultList } from './SearchResultList';
import { isListSubset } from '../entity/shared/utils';
import { EntityAndType } from '../entity/shared/types';
import { ErrorSection } from '../shared/error/ErrorSection';
import { UnionType } from './utils/constants';
import { DownloadSearchResults, DownloadSearchResultsInput } from './utils/types';
import { SidebarProvider } from './sidebar/SidebarContext';
import { BrowseProvider } from './sidebar/BrowseContext';
import { useIsBrowseV2, useIsSearchV2 } from './useSearchAndBrowseVersion';
import { combineSiblingsInSearchResults } from './utils/combineSiblingsInSearchResults';
import BrowseSidebar from './sidebar';
import SearchResultsLoadingSection from './SearchResultsLoadingSection';
import { SearchEntitySidebarContainer } from './SearchEntitySidebarContainer';
import { ANTD_GRAY } from '../entityV2/shared/constants';
import { PreviewType } from '../entity/Entity';
import { useIsShowSeparateSiblingsEnabled } from '../useAppConfig';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import { formatNumberWithoutAbbreviation } from '../shared/formatNumber';

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

const LeftControlsContainer = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
    justify-content: center;
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
    padding-top: 16px;
    padding-bottom: 16px;
    text-align: center;
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
    _unionType?: UnionType;
    query: string;
    _viewUrn?: string;
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
    _availableFilters: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    error: any;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
    _downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
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
    _unionType = UnionType.AND,
    query,
    _viewUrn,
    page,
    searchResponse,
    _availableFilters,
    selectedFilters,
    error,
    onChangeFilters,
    onChangePage,
    _downloadSearchResults,
    numResultsPerPage,
    setNumResultsPerPage,
    isSelectMode,
    selectedEntities,
    areAllEntitiesSelected,
    setAreAllEntitiesSelected,
    suggestions,
    setIsSelectMode,
    setSelectedEntities,
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

    function handlePageChange(p: number) {
        onChangePage(p);
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
                                            <LeftControlsContainer>
                                                <Typography.Text>
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
                                                </Typography.Text>
                                            </LeftControlsContainer>
                                            {totalResults > 0 && (
                                                <PaginationControlContainer id="search-pagination">
                                                    <Pagination
                                                        current={page}
                                                        pageSize={numResultsPerPage}
                                                        total={totalResults}
                                                        showLessItems
                                                        onChange={handlePageChange}
                                                        showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                                                        onShowSizeChange={(_currNum, newNum) =>
                                                            setNumResultsPerPage(newNum)
                                                        }
                                                        pageSizeOptions={['10', '20', '50', '100']}
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
