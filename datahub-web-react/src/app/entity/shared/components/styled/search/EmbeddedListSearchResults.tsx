import React from 'react';
import { Button, Pagination, Spin, Typography } from 'antd';
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
import { useIsShowSeparateSiblingsEnabled } from '../../../../../useAppConfig';
import { combineSiblingsInSearchResults } from '../../../../../search/utils/combineSiblingsInSearchResults';

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
    font-size: 32px;
    color: ${ANTD_GRAY[7]};
    padding-bottom: 18px;
]`;

const ErrorMessage = styled.div`
    padding-top: 70px;
    font-size: 16px;
    padding-bottom: 40px;
    width: 100%;
    text-align: center;
    flex: 1;
`;

const StyledLinkButton = styled(Button)`
    margin: 0 -14px;
    font-size: 16px;
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
    singleSelect?: boolean;
    entityAction?: React.FC<EntityActionProps>;
    applyView?: boolean;
    isServerOverloadError?: any;
    onClickLessHops?: () => void;
    onLineageClick?: () => void;
    isLineageTab?: boolean;
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
    singleSelect,
    entityAction,
    applyView,
    isServerOverloadError,
    onClickLessHops,
    onLineageClick,
    isLineageTab = false,
}: Props) => {
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const combinedSiblingSearchResults = combineSiblingsInSearchResults(
        showSeparateSiblings,
        searchResponse?.searchResults,
    );

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
                            <Spin indicator={<StyledLoading />} />
                        </LoadingContainer>
                    )}
                    {isLineageTab && !loading && isServerOverloadError && (
                        <ErrorMessage>
                            Data is too large. Please use
                            <StyledLinkButton onClick={onLineageClick} type="link">
                                visualize lineage
                            </StyledLinkButton>
                            or see less hops by clicking
                            <StyledLinkButton onClick={onClickLessHops} type="link">
                                here
                            </StyledLinkButton>
                        </ErrorMessage>
                    )}
                    {!loading && !isServerOverloadError && (
                        <EntitySearchResults
                            searchResults={combinedSiblingSearchResults || []}
                            additionalPropertiesList={
                                combinedSiblingSearchResults?.map((searchResult) => ({
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
                            singleSelect={singleSelect}
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
