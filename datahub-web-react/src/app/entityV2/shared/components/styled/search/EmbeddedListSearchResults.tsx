import { LoadingOutlined } from '@ant-design/icons';
import LanguageIcon from '@mui/icons-material/Language';
import { Pagination, Spin, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { SearchCfg } from '../../../../../../conf';
import {
    DataHubView,
    FacetFilterInput,
    FacetMetadata,
    SearchResults as SearchResultType,
} from '../../../../../../types.generated';
import { EntityAndType } from '../../../../../entity/shared/types';
import { SearchFiltersSection } from '../../../../../search/SearchFiltersSection';
import { UnionType } from '../../../../../search/utils/constants';
import { combineSiblingsInSearchResults } from '../../../../../searchV2/utils/combineSiblingsInSearchResults';
import { useIsShowSeparateSiblingsEnabled } from '../../../../../useAppConfig';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';
import { EntityActionProps, EntitySearchResults } from './EntitySearchResults';
import MatchingViewsLabel from './MatchingViewsLabel';

const SearchBody = styled.div`
    height: 100%;
    overflow-y: auto;
    display: flex;
    background-color: ${REDESIGN_COLORS.BACKGROUND};
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const FiltersContainer = styled.div`
    background-color: ${REDESIGN_COLORS.WHITE};
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
    position: relative;
    width: 100%;
    display: flex;
    flex-direction: column;
`;

const PaginationInfoContainer = styled.span`
    padding: 8px;
    padding-left: 16px;
    border-top: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
    overflow: auto;
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
`;

const ViewsContainer = styled.div`
    background-color: ${REDESIGN_COLORS.BORDER_2};
    padding: 10px 16px;
    width: 100%;
    display: flex;
    align-items: center;
    gap: 1rem;
`;

const Pill = styled.div<{ selected?: boolean }>`
    border: 1px solid ${(props) => (props.selected ? REDESIGN_COLORS.TITLE_PURPLE : `#797F98`)};
    white-space: nowrap;
    border-radius: 20px;
    padding: 5px 16px;
    color: ${(props) => (props.selected ? REDESIGN_COLORS.TITLE_PURPLE : '#797F98')};
    cursor: pointer;
    display: flex;
    gap: 0.5rem;
    align-items: center;
    background: ${(props) => (props.selected ? '#E5E2F8' : 'none')};
`;

const Count = styled.div<{ selected: boolean }>`
    background-color: ${(props) => (props.selected ? REDESIGN_COLORS.HOVER_PURPLE : '#A3A7B9')};
    color: ${REDESIGN_COLORS.WHITE};
    border-radius: 20px;
    min-width: 25px;
    padding: 2px 4px;

    display: flex;
    align-items: center;
    justify-content: center;

    font-size: 11px;
`;

const LanguageIconStyle = styled(LanguageIcon)<{ selected?: boolean }>`
    font-size: 18px !important;
    color: ${(props) => (props.selected ? REDESIGN_COLORS.TITLE_PURPLE : '#797F98')};
`;

const ViewLabel = styled.span`
    font-weight: 700;
    color: #5f6685;
    font-size: 16px;
    margin-right: 8px;
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
    entityAction?: React.FC<EntityActionProps>;
    applyView?: boolean;
    selectedViewUrn?: string;
    setSelectedViewUrn?: (selectedViewUrn: string | undefined) => void;
    compactUserSearchCardStyle?: boolean;
    defaultViewUrn?: string | undefined;
    defaultViewCount?: number;
    allSearchCount?: number;
    view?: DataHubView;
    errorMessage?: string;
    selectLimit?: number;
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
    compactUserSearchCardStyle,
    selectedViewUrn,
    setSelectedViewUrn,
    defaultViewUrn,
    defaultViewCount = 0,
    allSearchCount = 0,
    view,
    errorMessage,
    selectLimit,
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
                    {view && (
                        <ViewsContainer>
                            <ViewLabel>View</ViewLabel>
                            <Pill selected={!selectedViewUrn} onClick={() => setSelectedViewUrn?.(undefined)}>
                                <LanguageIconStyle selected={!selectedViewUrn} />
                                <span>All</span>
                                {allSearchCount > 0 && <Count selected={!selectedViewUrn}>{allSearchCount}</Count>}
                            </Pill>
                            {defaultViewUrn === view.urn && (
                                <Pill
                                    selected={selectedViewUrn === view.urn}
                                    onClick={() => setSelectedViewUrn?.(view?.urn)}
                                >
                                    <span>{view.name}</span>
                                    {defaultViewCount > 0 && (
                                        <Count selected={selectedViewUrn === view.urn}>{defaultViewCount}</Count>
                                    )}
                                </Pill>
                            )}
                        </ViewsContainer>
                    )}
                    {loading && (
                        <LoadingContainer>
                            <Spin indicator={<StyledLoading />} />
                        </LoadingContainer>
                    )}
                    {!loading && (
                        <EntitySearchResults
                            noResultsMessage={errorMessage}
                            searchResults={combinedSiblingSearchResults || []}
                            additionalPropertiesList={
                                combinedSiblingSearchResults?.map((searchResult) => ({
                                    // when we add impact analysis, we will want to pipe the path to each element to the result this
                                    // eslint-disable-next-line @typescript-eslint/dot-notation
                                    degree: searchResult['degree'], // eslint-disable-next-line @typescript-eslint/dot-notation
                                    paths: searchResult['paths'],
                                })) || []
                            }
                            isSelectMode={isSelectMode}
                            selectLimit={selectLimit}
                            selectedEntities={selectedEntities}
                            setSelectedEntities={setSelectedEntities}
                            bordered={false}
                            entityAction={entityAction}
                            compactUserSearchCardStyle={compactUserSearchCardStyle}
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
                {applyView ? (
                    <MatchingViewsLabel
                        view={view}
                        selectedViewUrn={selectedViewUrn}
                        setSelectedViewUrn={setSelectedViewUrn}
                    />
                ) : (
                    <span />
                )}
            </PaginationInfoContainer>
        </>
    );
};
