import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { colors } from '@components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Rows, List } from 'phosphor-react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { SEARCH_RESULTS_FILTERS_ID } from '../../onboarding/config/SearchOnboardingConfig';
import SearchFilterOptions from './SearchFilterOptions';
import SearchSortSelect from '../sorting/SearchSortSelect';
import SelectedSearchFilters from './SelectedSearchFilters';
import SearchQuerySuggester from '../suggestions/SearchQuerySugggester';
import { RecommendedFilters } from '../recommendation/RecommendedFilters';
import SearchMenuItems from '../../sharedV2/search/SearchMenuItems';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useSearchContext } from '../../search/context/SearchContext';
import {
    ENTITY_INDEX_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UnionType,
    LEGACY_ENTITY_FILTER_NAME,
    BROWSE_PATH_V2_FILTER_NAME,
    COMPLETED_FORMS_FILTER_NAME,
    INCOMPLETE_FORMS_FILTER_NAME,
    VERIFIED_FORMS_FILTER_NAME,
    COMPLETED_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    INCOMPLETE_FORMS_COMPLETED_PROMPT_IDS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    PROPOSED_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_TAGS_FILTER_NAME,
} from '../utils/constants';
import { generateOrFilters } from '../utils/generateOrFilters';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${colors.white};
    display: flex;
    flex-direction: column;
    gap: 12px;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    padding: 16px;
    border: 1px solid ${colors.gray[100]};
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['box-shadow-navbar-redesign'] : '0px 4px 10px 0px #a8a8a840'};
`;

const FilterandMenuContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SearchMenuContainer = styled.div`
    display: flex;
    align-items: center;
`;

const CustomSwitch = styled.div`
    background: ${colors.gray[1600]};
    border: 1px solid ${colors.gray[100]};
    border-radius: 30px;
    display: flex;
    gap: 2px;
    align-items: center;
    padding: 2px;
    width: fit-content;
    justify-content: space-between;
    margin-left: 8px;
}
`;

const IconContainer = styled.div<{ isActive?: boolean }>`
    cursor: pointer;
    align-items: center;
    display: flex;
    padding: 4px;
    transition: left 0.5s ease;

    ${(props) =>
        props.isActive &&
        `
        background: ${REDESIGN_COLORS.TITLE_PURPLE};
        border-radius: 100%;
        color: white;
    `}
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

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    basicFilters?: boolean;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
    onClearFilters: () => void;
    query?: string;
    totalResults?: number;
    _page?: number;
    _pageSize?: number;
    suggestions?: any[];
    _downloadSearchResults?: any;
    _viewUrn?: string;
    setIsSelectMode?: (showSelectMode: boolean) => any;
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
    query = '',
    totalResults = 0,
    _page,
    _pageSize,
    suggestions = [],
    _downloadSearchResults,
    _viewUrn,
    setIsSelectMode,
}: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { selectedSortOption, setSelectedSortOption, isFullViewCard, setIsFullViewCard } = useSearchContext();
    // Filter out the available filters if `basicFilters` is true
    const filteredFilters = (availableFilters || []).filter((f) => !FILTERS_TO_REMOVE.includes(f.field));
    const filters = basicFilters ? filteredFilters : availableFilters;

    return (
        <Container id={SEARCH_RESULTS_FILTERS_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <FilterandMenuContainer>
                <SearchFilterOptions
                    loading={loading}
                    availableFilters={filters}
                    activeFilters={activeFilters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                />
                {totalResults > 0 && <SearchQuerySuggester suggestions={suggestions} />}
                <SearchMenuContainer>
                    <SearchSortSelect
                        selectedSortOption={selectedSortOption}
                        setSelectedSortOption={setSelectedSortOption}
                    />
                    <SearchMenuItems
                        downloadSearchResults={_downloadSearchResults}
                        filters={generateOrFilters(unionType, activeFilters)}
                        query={query}
                        viewUrn={_viewUrn}
                        setShowSelectMode={setIsSelectMode}
                        totalResults={totalResults}
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
                </SearchMenuContainer>
            </FilterandMenuContainer>
            <RecommendedFilters
                availableFilters={availableFilters || []}
                selectedFilters={activeFilters}
                onChangeFilters={onChangeFilters}
            />
            {activeFilters.length > 0 && (
                <>
                    <SelectedSearchFilters
                        availableFilters={filters}
                        selectedFilters={activeFilters}
                        unionType={unionType}
                        onChangeFilters={onChangeFilters}
                        onChangeUnionType={onChangeUnionType}
                        onClearFilters={onClearFilters}
                        showUnionType
                    />
                </>
            )}
        </Container>
    );
}
