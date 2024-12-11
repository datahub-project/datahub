import { AggregateAcrossEntitiesQuery } from '../../../../graphql/search.generated';
import { AggregationMetadata, FacetFilterInput, FacetMetadata } from '../../../../types.generated';
import EntityRegistry from '../../../entity/EntityRegistry';
import {
    LEGACY_ENTITY_FILTER_FIELDS,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ENTITY_FILTER_NAME,
    FILTER_DELIMITER,
} from '../../utils/constants';
import { mapFilterOption } from '../mapFilterOption';
import { FilterOptionType } from '../types';
import { filterOptionsWithSearch, getFilterIconAndLabel, getFilterOptions } from '../utils';

const BACKWARDS_COMPATIBLE_FILTER_FIELDS = [ENTITY_SUB_TYPE_FILTER_NAME, ...LEGACY_ENTITY_FILTER_FIELDS];

export function getInitialSelectedOptionsFromAggregations(
    aggregations: AggregationMetadata[],
    activeFilterValues: string[],
    initialSelectedOptions: FilterOptionType[],
) {
    return aggregations
        .filter((agg) => activeFilterValues?.includes(agg.value))
        .filter((agg) => !initialSelectedOptions?.find((initialFilter) => initialFilter.value === agg.value)) // make sure we don't have duplicates
        .map((agg) => ({
            field: ENTITY_SUB_TYPE_FILTER_NAME,
            value: agg.value,
            entity: agg.entity,
            count: agg.count,
        }));
}

export function getInitialSelectedOptions(activeFilters: FacetFilterInput[], data?: AggregateAcrossEntitiesQuery) {
    let initialSelectedOptions: FilterOptionType[] = [];
    const activeFilterValues = activeFilters.find((f) => BACKWARDS_COMPATIBLE_FILTER_FIELDS.includes(f.field))?.values;
    data?.aggregateAcrossEntities?.facets?.forEach((facet) => {
        if (BACKWARDS_COMPATIBLE_FILTER_FIELDS.includes(facet.field)) {
            initialSelectedOptions = [
                ...initialSelectedOptions,
                ...getInitialSelectedOptionsFromAggregations(
                    facet.aggregations,
                    activeFilterValues || [],
                    initialSelectedOptions,
                ),
            ];
        }
    });

    return initialSelectedOptions;
}

function addAggregationIfNotPresent(aggregations: AggregationMetadata[], facet: FacetMetadata) {
    facet.aggregations.forEach((agg) => {
        if (!aggregations.find((a) => a.value === agg.value)) {
            aggregations.push(agg);
        }
    });
}

function getAggregationsForFilterOptions(data?: AggregateAcrossEntitiesQuery) {
    const aggregations: AggregationMetadata[] = [];
    data?.aggregateAcrossEntities?.facets?.forEach((facet) => {
        if (facet.field === ENTITY_SUB_TYPE_FILTER_NAME) {
            addAggregationIfNotPresent(aggregations, facet);
        } else if (facet.field === ENTITY_FILTER_NAME) {
            addAggregationIfNotPresent(aggregations, facet);
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
        .filter((option) => !option.value.includes(FILTER_DELIMITER)) // remove nested options
        .map((filterOption) => {
            const nestedOptions = aggregations
                .filter(
                    (option) => option.value.includes(FILTER_DELIMITER) && option.value.includes(filterOption.value),
                )
                .map((option) => ({ field: ENTITY_SUB_TYPE_FILTER_NAME, ...option } as FilterOptionType))
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
