import { Tooltip, colors } from '@components';
import { List, Rows } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { SEARCH_RESULTS_FILTERS_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useSearchContext } from '@app/search/context/SearchContext';
import SearchFilterOptions from '@app/searchV2/filters/SearchFilterOptions';
import SelectedSearchFilters from '@app/searchV2/filters/SelectedSearchFilters';
import { RecommendedFilters } from '@app/searchV2/recommendation/RecommendedFilters';
import { useGetRecommendedFilters } from '@app/searchV2/recommendation/useGetRecommendedFilters';
import SearchSortSelect from '@app/searchV2/sorting/SearchSortSelect';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    COMPLETED_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    COMPLETED_FORMS_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    INCOMPLETE_FORMS_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    PROPOSED_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UnionType,
    VERIFIED_FORMS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/searchV2/utils/types';
import SearchMenuItems from '@app/sharedV2/search/SearchMenuItems';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { FacetFilterInput, FacetMetadata } from '@types';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${colors.white};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    padding: 16px 0px 8px 0px;
    border: 1px solid ${colors.gray[100]};
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['box-shadow-navbar-redesign'] : '0px 4px 10px 0px #a8a8a840'};
`;

const FiltersContainerTop = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    padding-left: 16px;
    padding-right: 16px;
    padding-bottom: 8px;
`;

const CustomSwitch = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 30px;
    display: flex;
    gap: 2px;
    align-items: center;
    padding: 2px;
    width: fit-content;
    justify-content: space-between;
`;

const IconContainer = styled.div<{ isActive?: boolean }>`
    cursor: pointer;
    align-items: center;
    display: flex;
    padding: 4px;
    transition: left 0.5s ease;
    color: ${colors.gray[1800]};

    ${(props) =>
        props.isActive &&
        `
        background: ${colors.gray[100]};
        border-radius: 100%;
        color: ${colors.gray[1700]};
    `}
`;

const SelectedFiltersContainer = styled.div`
    padding: 4px 16px 8px 16px;
`;

// remove legacy filter options as well as new _index and browsePathV2 filter from dropdowns
const FILTERS_TO_REMOVE = [
    TYPE_NAMES_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    // remove form-related filters for bulk form search and browse experience
    COMPLETED_FORMS_FILTER_NAME,
    INCOMPLETE_FORMS_FILTER_NAME,
    VERIFIED_FORMS_FILTER_NAME,
    COMPLETED_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    PROPOSED_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_TAGS_FILTER_NAME,
];

const ControlsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    align-self: start;
`;

const RecommendedFiltersContainer = styled.div`
    border-top: 1px solid ${colors.gray[100]};
    padding: 16px 16px 8px 16px;
`;

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    basicFilters?: boolean;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onClearFilters: () => void;
    query: string;
    viewUrn?: string;
    totalResults?: number;
    setShowSelectMode?: (showSelectMode: boolean) => any;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
}

export default function SearchFilters({
    loading,
    availableFilters,
    activeFilters,
    unionType,
    basicFilters = false,
    onChangeFilters,
    onChangeUnionType,
    onClearFilters,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
    downloadSearchResults,
}: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { isFullViewCard, setIsFullViewCard, selectedSortOption, setSelectedSortOption } = useSearchContext();
    // Filter out the available filters if `basicFilters` is true
    const filteredFilters = (availableFilters || []).filter((f) => !FILTERS_TO_REMOVE.includes(f.field));
    const filters = basicFilters ? filteredFilters : availableFilters;
    const recommendedFilters = useGetRecommendedFilters(filters, activeFilters);

    return (
        <Container id={SEARCH_RESULTS_FILTERS_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <FiltersContainerTop>
                <SearchFilterOptions
                    loading={loading}
                    availableFilters={filters}
                    activeFilters={activeFilters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                />
                <ControlsContainer>
                    <SearchSortSelect
                        selectedSortOption={selectedSortOption}
                        setSelectedSortOption={setSelectedSortOption}
                    />
                    <SearchMenuItems
                        filters={generateOrFilters(unionType, activeFilters)}
                        query={query}
                        viewUrn={viewUrn}
                        totalResults={totalResults || 0}
                        setShowSelectMode={setShowSelectMode}
                        downloadSearchResults={downloadSearchResults}
                    />
                    <CustomSwitch>
                        <IconContainer isActive={isFullViewCard} onClick={() => setIsFullViewCard(true)}>
                            <Tooltip showArrow={false} title="Full Card View">
                                <Rows
                                    style={{
                                        fontSize: '16px',
                                    }}
                                />
                            </Tooltip>
                        </IconContainer>
                        <IconContainer isActive={!isFullViewCard} onClick={() => setIsFullViewCard(false)}>
                            <Tooltip showArrow={false} title="Compact Card View">
                                <List
                                    style={{
                                        fontSize: '16px',
                                    }}
                                />
                            </Tooltip>
                        </IconContainer>
                    </CustomSwitch>
                </ControlsContainer>
            </FiltersContainerTop>
            {activeFilters.length > 0 && (
                <SelectedFiltersContainer>
                    <SelectedSearchFilters
                        availableFilters={filters}
                        selectedFilters={activeFilters}
                        unionType={unionType}
                        onChangeFilters={onChangeFilters}
                        onChangeUnionType={onChangeUnionType}
                        onClearFilters={onClearFilters}
                        showUnionType
                    />
                </SelectedFiltersContainer>
            )}
            {(totalResults || 0) > 0 && recommendedFilters.length > 0 && (
                <RecommendedFiltersContainer>
                    <RecommendedFilters
                        availableFilters={filters}
                        selectedFilters={activeFilters}
                        onChangeFilters={onChangeFilters}
                    />
                </RecommendedFiltersContainer>
            )}
        </Container>
    );
}
