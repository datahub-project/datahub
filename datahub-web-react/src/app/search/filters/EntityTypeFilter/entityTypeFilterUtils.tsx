import { AggregateAcrossEntitiesQuery } from '../../../../graphql/search.generated';
import { AggregationMetadata, FacetFilterInput } from '../../../../types.generated';
import EntityRegistry from '../../../entity/EntityRegistry';
import {
    LEGACY_ENTITY_FILTER_FIELDS,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ENTITY_TYPE_FILTER_NAME,
} from '../../utils/constants';
import { mapFilterOption } from '../mapFilterOption';
import { FilterOptionType } from '../types';
import { filterOptionsWithSearch, getFilterIconAndLabel, getFilterOptions } from '../utils';

const BACKWARDS_COMPATIBLE_FILTER_FIELDS = [ENTITY_SUB_TYPE_FILTER_NAME, ...LEGACY_ENTITY_FILTER_FIELDS];

function getAggregationsForFilterOptions(data?: AggregateAcrossEntitiesQuery) {
    let aggregations: AggregationMetadata[] = [];
    data?.aggregateAcrossEntities?.facets?.forEach((facet) => {
        if (facet.field === ENTITY_SUB_TYPE_FILTER_NAME) {
            aggregations = [...aggregations, ...facet.aggregations];
        } else if (facet.field === ENTITY_TYPE_FILTER_NAME) {
            facet.aggregations.forEach((agg) => {
                if (!aggregations.find((a) => a.value === agg.value)) {
                    aggregations.push(agg);
                }
            });
        }
    });
    return aggregations;
}

function filterNestedOptions(nestedOption: FilterOptionType, entityRegistry: EntityRegistry, searchQuery: string) {
    const { label } = getFilterIconAndLabel(ENTITY_SUB_TYPE_FILTER_NAME, nestedOption.value, entityRegistry, null);
    return filterOptionsWithSearch(searchQuery, (label as string) || '');
}

// transforms options to be displayed and filters them based in the search input the user types in
export function getDisplayedFilterOptions(
    selectedFilterOptions: FilterOptionType[],
    entityRegistry: EntityRegistry,
    setSelectedFilterOptions: (values: FilterOptionType[]) => void,
    searchQuery: string,
    data?: AggregateAcrossEntitiesQuery,
) {
    const aggregations = getAggregationsForFilterOptions(data);

    // map aggregations to filter options, removing nested options, and placing nested options under top level options
    const filterOptions = getFilterOptions(ENTITY_SUB_TYPE_FILTER_NAME, aggregations, selectedFilterOptions, [] as any)
        .filter((option) => !option.value.includes('␞')) // remove nested options
        .map((filterOption) => {
            const nestedOptions = aggregations
                .filter((option) => option.value.includes('␞') && option.value.includes(filterOption.value))
                .map((option) => ({ field: ENTITY_SUB_TYPE_FILTER_NAME, ...option }))
                .filter((o) => filterNestedOptions(o, entityRegistry, searchQuery));
            return mapFilterOption({
                filterOption,
                entityRegistry,
                selectedFilterOptions,
                setSelectedFilterOptions,
                nestedOptions,
            });
        })
        .filter((option) => filterOptionsWithSearch(searchQuery, option.displayName as string, option.nestedOptions));

    return filterOptions;
}

export function getNumActiveFilters(activeFilters: FacetFilterInput[]) {
    const activeFilterValues = new Set(
        activeFilters.filter((f) => BACKWARDS_COMPATIBLE_FILTER_FIELDS.includes(f.field)).flatMap((f) => f.values),
    );
    return activeFilterValues.size;
}