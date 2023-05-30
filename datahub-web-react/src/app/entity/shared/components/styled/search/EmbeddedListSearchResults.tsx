import React from 'react';
import { Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, SearchResults as SearchResultType } from '../../../../../../types.generated';
import { SearchCfg } from '../../../../../../conf';
import { EntityNameList } from '../../../../../recommendations/renderer/component/EntityNameList';
import { ReactComponent as LoadingSvg } from '../../../../../../images/datahub-logo-color-loading_pendulum.svg';
import { EntityAndType } from '../../../types';
import { UnionType } from '../../../../../search/utils/constants';
import { SearchFiltersSection } from '../../../../../search/SearchFiltersSection';

const SearchBody = styled.div`
    height: 100%;
    overflow-y: auto;
    display: flex;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const FiltersContainer = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    max-width: 260px;
    min-width: 260px;
    border-right: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const ResultContainer = styled.div`
    height: auto;
    overflow: auto;
    flex: 1;
`;

const PaginationInfoContainer = styled.span`
    padding: 8px;
    padding-left: 16px;
    border-top: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 0px;
    padding: 0px;
`;

const LoadingContainer = styled.div`
    padding-top: 40px;
    padding-bottom: 40px;
    width: 100%;
    text-align: center;
    flex: 1;
`;

interface Props {
    page: number;
    searchResponse?: SearchResultType | null;
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    loading: boolean;
    showFilters?: boolean;
    unionType: UnionType;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => any;
    numResultsPerPage: number;
    setNumResultsPerPage: (numResults: number) => void;
}

export const EmbeddedListSearchResults = ({
    page,
    searchResponse,
    filters,
    selectedFilters,
    loading,
    showFilters,
    unionType,
    onChangeUnionType,
    onChangeFilters,
    onChangePage,
    isSelectMode,
    selectedEntities,
    setSelectedEntities,
    numResultsPerPage,
    setNumResultsPerPage,
}: Props) => {
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;

    return (
        <>
            <SearchBody>
                {!!showFilters && (
                    <FiltersContainer>
                        <SearchFiltersSection
                            filters={filters}
                            selectedFilters={selectedFilters}
                            unionType={unionType}
                            loading={loading}
                            onChangeFilters={onChangeFilters}
                            onChangeUnionType={onChangeUnionType}
                        />
                    </FiltersContainer>
                )}
                <ResultContainer>
                    {loading && (
                        <LoadingContainer>
                            <LoadingSvg height={80} width={80} />
                        </LoadingContainer>
                    )}
                    {!loading && (
                        <EntityNameList
                            entities={searchResponse?.searchResults?.map((searchResult) => searchResult.entity) || []}
                            additionalPropertiesList={
                                searchResponse?.searchResults?.map((searchResult) => ({
                                    // when we add impact analysis, we will want to pipe the path to each element to the result this
                                    // eslint-disable-next-line @typescript-eslint/dot-notation
                                    degree: searchResult['degree'],
                                    // eslint-disable-next-line @typescript-eslint/dot-notation
                                    paths: searchResult['paths'],
                                })) || []
                            }
                            isSelectMode={isSelectMode}
                            selectedEntities={selectedEntities}
                            setSelectedEntities={setSelectedEntities}
                            bordered={false}
                        />
                    )}
                </ResultContainer>
            </SearchBody>
            <PaginationInfoContainer>
                <PaginationInfo>
                    <b>
                        {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                    </b>{' '}
                    of <b>{totalResults}</b>
                </PaginationInfo>
                <StyledPagination
                    current={page}
                    pageSize={numResultsPerPage}
                    total={totalResults}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={totalResults > SearchCfg.RESULTS_PER_PAGE}
                    onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage(newNum)}
                    pageSizeOptions={['10', '20', '50', '100']}
                />
                <span />
            </PaginationInfoContainer>
        </>
    );
};
