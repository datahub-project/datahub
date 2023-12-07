import React from 'react';
import { Pagination, Typography } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata, SearchResults as SearchResultType } from '../../../../../../types.generated';
import { SearchCfg } from '../../../../../../conf';
import { EntityAndType } from '../../../types';
import { UnionType } from '../../../../../search/utils/constants';
import { SearchFiltersSection } from '../../../../../search/SearchFiltersSection';
import { EntitySearchResults, EntityActionProps } from './EntitySearchResults';
import MatchingViewsLabel from './MatchingViewsLabel';
import { ANTD_GRAY } from '../../../constants';

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

const StyledLoading = styled(LoadingOutlined)`
    font-size: 36px;
    color: ${ANTD_GRAY[7]};
    padding-bottom: 18px;
]`;

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
    entityAction?: React.FC<EntityActionProps>;
    applyView?: boolean;
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
    entityAction,
    applyView,
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
                            <StyledLoading />
                        </LoadingContainer>
                    )}
                    {!loading && (
                        <EntitySearchResults
                            searchResults={searchResponse?.searchResults || []}
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
                            entityAction={entityAction}
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
                {applyView ? <MatchingViewsLabel /> : <span />}
            </PaginationInfoContainer>
        </>
    );
};
