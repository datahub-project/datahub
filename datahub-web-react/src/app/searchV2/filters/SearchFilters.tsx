import React from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { SEARCH_RESULTS_FILTERS_ID } from '../../onboarding/config/SearchOnboardingConfig';
import SearchFilterOptions from './SearchFilterOptions';
import SelectedSearchFilters from './SelectedSearchFilters';
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

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    padding: 16px 24px 16px 32px;
    border: 1px solid #e8e8e8;
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['box-shadow-navbar-redesign'] : '0px 4px 10px 0px #a8a8a840'};
`;

const FilterSpacer = styled.div`
    margin: 16px 0px;
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
}: Props) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    // Filter out the available filters if `basicFilters` is true
    const filteredFilters = (availableFilters || []).filter((f) => !FILTERS_TO_REMOVE.includes(f.field));
    const filters = basicFilters ? filteredFilters : availableFilters;

    return (
        <Container id={SEARCH_RESULTS_FILTERS_ID} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <SearchFilterOptions
                loading={loading}
                availableFilters={filters}
                activeFilters={activeFilters}
                unionType={unionType}
                onChangeFilters={onChangeFilters}
            />
            {activeFilters.length > 0 && (
                <>
                    <FilterSpacer />
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
