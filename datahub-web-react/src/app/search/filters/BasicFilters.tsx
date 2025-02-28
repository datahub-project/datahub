import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { useUserContext } from '../../context/useUserContext';
import {
    ENTITY_INDEX_FILTER_NAME,
    ORIGIN_FILTER_NAME,
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
} from '../utils/constants';
import ActiveFilter from './ActiveFilter';
import { SORTED_FILTERS } from './constants';
import MoreFilters from './MoreFilters';
import SaveViewButton from './SaveViewButton';
import SearchFilter from './SearchFilter';
import { TextButton } from './styledComponents';
import { sortFacets } from './utils';
import {
    SEARCH_RESULTS_ADVANCED_SEARCH_ID,
    SEARCH_RESULTS_FILTERS_ID,
} from '../../onboarding/config/SearchOnboardingConfig';
import { useFilterRendererRegistry } from './render/useFilterRenderer';
import { FilterScenarioType } from './render/types';
import { FormView, useEntityFormContext } from '../../entity/shared/entityForm/EntityFormContext';
import FormPromptFilter from './FormPromptFilter';
import FormResponsesActiveFilters from './FormResponsesActiveFilters';
import BasicFiltersLoadingSection from './BasicFiltersLoadingSection';

const NUM_VISIBLE_FILTER_DROPDOWNS = 5;

export const FlexWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
`;

export const FlexSpacer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const StyledDivider = styled(Divider)`
    margin: 8px 0 0 0;
`;

export const FilterButtonsWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
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
];

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[] | null;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onClearFilters: () => void;
    showAdvancedFilters: () => void;
}

export default function BasicFilters({
    loading,
    availableFilters,
    activeFilters,
    onChangeFilters,
    onClearFilters,
    showAdvancedFilters,
}: Props) {
    const {
        isInFormContext,
        filter: { formResponsesFilters },
        form: { formView },
    } = useEntityFormContext();
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveViewButton = activeFilters?.length > 0 && selectedViewUrn === undefined;
    // only want Environment filter if there's 2 or more envs
    const filters = availableFilters
        ?.filter((f) => (f.field === ORIGIN_FILTER_NAME ? f.aggregations.length >= 2 : true)) // only want Environment filter if there's 2 or more envs
        .filter((f) => !FILTERS_TO_REMOVE.includes(f.field))
        .sort((facetA, facetB) => sortFacets(facetA, facetB, SORTED_FILTERS));
    // if there will only be one filter in the "More Filters" dropdown, show that filter instead
    const numVisibleFilterDropdowns = isInFormContext ? NUM_VISIBLE_FILTER_DROPDOWNS - 1 : NUM_VISIBLE_FILTER_DROPDOWNS;
    const shouldShowMoreDropdown = filters && filters.length > numVisibleFilterDropdowns + 1;
    const visibleFilters = shouldShowMoreDropdown ? filters?.slice(0, numVisibleFilterDropdowns) : filters;
    const hiddenFilters = shouldShowMoreDropdown ? filters?.slice(numVisibleFilterDropdowns) : [];
    const filterRendererRegistry = useFilterRendererRegistry();
    const isShowingLoadingSection = loading && !visibleFilters?.length;
    const isShowingFormPromptFilter = isInFormContext && formView !== FormView.BULK_VERIFY && !isShowingLoadingSection;

    return (
        <span id={SEARCH_RESULTS_FILTERS_ID}>
            <FlexSpacer>
                <FlexWrapper>
                    {isShowingFormPromptFilter && <FormPromptFilter />}
                    {isShowingLoadingSection && <BasicFiltersLoadingSection />}
                    {visibleFilters?.map((filter) => {
                        return filterRendererRegistry.hasRenderer(filter.field) ? (
                            filterRendererRegistry.render(filter.field, {
                                scenario: FilterScenarioType.SEARCH_V2_PRIMARY,
                                filter,
                                activeFilters,
                                onChangeFilters,
                            })
                        ) : (
                            <SearchFilter
                                key={filter.field}
                                filter={filter}
                                activeFilters={activeFilters}
                                onChangeFilters={onChangeFilters}
                            />
                        );
                    })}
                    {hiddenFilters && hiddenFilters.length > 0 && (
                        <MoreFilters
                            filters={hiddenFilters}
                            activeFilters={activeFilters}
                            onChangeFilters={onChangeFilters}
                        />
                    )}
                </FlexWrapper>
                <FilterButtonsWrapper>
                    {showSaveViewButton && <SaveViewButton activeFilters={activeFilters} unionType={UnionType.AND} />}
                    <TextButton
                        id={SEARCH_RESULTS_ADVANCED_SEARCH_ID}
                        type="text"
                        onClick={showAdvancedFilters}
                        marginTop={0}
                    >
                        Advanced Filters
                    </TextButton>
                </FilterButtonsWrapper>
            </FlexSpacer>
            {(activeFilters.length > 0 || (!!formResponsesFilters?.length && isShowingFormPromptFilter)) && (
                <>
                    <StyledDivider />
                    <FlexSpacer>
                        <FlexWrapper>
                            {isShowingFormPromptFilter && <FormResponsesActiveFilters />}
                            {activeFilters.map((activeFilter) => (
                                <ActiveFilter
                                    key={activeFilter.field}
                                    filter={activeFilter}
                                    availableFilters={availableFilters}
                                    activeFilters={activeFilters}
                                    onChangeFilters={onChangeFilters}
                                />
                            ))}
                        </FlexWrapper>
                        <TextButton type="text" onClick={onClearFilters} height={14} data-testid="clear-all-filters">
                            clear all
                        </TextButton>
                    </FlexSpacer>
                </>
            )}
        </span>
    );
}
