import { Icon, Tooltip } from '@components';
import { List } from '@phosphor-icons/react/dist/csr/List';
import { Rows } from '@phosphor-icons/react/dist/csr/Rows';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import CreateLogicalModelButton from '@app/entityV2/shared/logicalModels/CreateLogicalModelButton';
import { SEARCH_RESULTS_FILTERS_ID } from '@app/onboarding/config/SearchOnboardingConfig';
import { useSearchContext } from '@app/search/context/SearchContext';
import SearchFilterBar from '@app/searchV2/filters/SearchFilterBar';
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
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    padding: 12px 0;
    border: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => (props.$isShowNavBarRedesign ? props.theme.colors.shadowSm : props.theme.colors.shadowXs)};
`;

const FiltersContainerTop = styled.div`
    display: flex;
    flex-direction: row;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    padding-left: 16px;
    padding-right: 16px;
`;

const CustomSwitch = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
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
    color: ${(props) => props.theme.colors.textTertiary};

    ${(props) =>
        props.isActive &&
        `
background: ${props.theme.colors.bgSurface};
 border-radius: 100%;
color: ${props.theme.colors.textSecondary};
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

const ControlsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    align-self: start;
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
    onClearFilters: _onClearFilters,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
    downloadSearchResults,
}: Props) {
    const { t } = useTranslation('search');
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { isFullViewCard, setIsFullViewCard, selectedSortOption, setSelectedSortOption } = useSearchContext();
    // Filter out the available filters if `basicFilters` is true
    const filteredFilters = (availableFilters || []).filter((f) => !FILTERS_TO_REMOVE.includes(f.field));
    const filters = basicFilters ? filteredFilters : availableFilters;

    return (
        <Container id={SEARCH_RESULTS_FILTERS_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <FiltersContainerTop>
                <SearchFilterBar
                    loading={loading}
                    availableFilters={filters}
                    activeFilters={activeFilters}
                    unionType={unionType}
                    onChangeFilters={onChangeFilters}
                    onChangeUnionType={onChangeUnionType}
                />
                <ControlsContainer>
                    <SearchSortSelect
                        selectedSortOption={selectedSortOption}
                        setSelectedSortOption={setSelectedSortOption}
                    />
                    <CreateLogicalModelButton />
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
                            <Tooltip showArrow={false} title={t('filters.fullCardView')}>
                                <Icon icon={Rows} size="md" />
                            </Tooltip>
                        </IconContainer>
                        <IconContainer isActive={!isFullViewCard} onClick={() => setIsFullViewCard(false)}>
                            <Tooltip showArrow={false} title={t('filters.compactCardView')}>
                                <Icon icon={List} size="md" />
                            </Tooltip>
                        </IconContainer>
                    </CustomSwitch>
                </ControlsContainer>
            </FiltersContainerTop>
        </Container>
    );
}
