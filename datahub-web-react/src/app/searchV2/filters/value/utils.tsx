import { useEffect, useState } from 'react';
import { EntityRegistry } from '../../../../entityRegistryContext';
import {
    AggregateAcrossEntitiesQuery,
    GetSearchResultsForMultipleQuery,
    useAggregateAcrossEntitiesLazyQuery,
    useGetSearchResultsForMultipleLazyQuery,
} from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { FILTER_DELIMITER } from '../../utils/constants';
import { FieldType, FilterField, FilterOperatorType, FilterValueOption } from '../types';
import { filterOptionsWithSearch } from '../utils';

const MAX_AGGREGATION_COUNT = 40;

/**
 * Simply maps the autocomplete results to the standard FilterValueOption to be used in the dropdown.
 */
const mapAutoCompleteOptions = (field: FilterField, result: GetSearchResultsForMultipleQuery): FilterValueOption[] => {
    return (
        result?.searchAcrossEntities?.searchResults?.map((res) => {
            const { entity } = res;
            return {
                value: entity.urn,
                entity,
                icon: field.icon,
            };
        }) || []
    );
};

/**
 * Simply maps the autocomplete results to the standard FilterValueOption to be used in the dropdown.
 */
const mapAggregateAcrossEntitiesOptions = (
    field: FilterField,
    result: AggregateAcrossEntitiesQuery,
    includeCounts: boolean,
): FilterValueOption[] => {
    const requestedAgg = result?.aggregateAcrossEntities?.facets?.find((facet: any) => facet.field === field.field);
    return (
        requestedAgg?.aggregations?.map((aggregation: any) => {
            return {
                value: aggregation.value,
                entity: aggregation.entity,
                icon: field.icon,
                count: includeCounts ? aggregation.count : undefined,
            };
        }) || []
    );
};

/**
 * Simpliy removes the baseOptions from moreOptions parameters in case the same filter option
 * appears in both lists.
 */
export const deduplicateOptions = (baseOptions: FilterValueOption[], moreOptions: FilterValueOption[]) => {
    const baseValues = baseOptions.map((op) => op.value);
    return moreOptions.filter((op) => !baseValues.includes(op.value));
};

export const mapFilterCountsToZero = (options: FilterValueOption[]) => {
    return options.map((option) => {
        return {
            ...option,
            count: 0,
        };
    });
};

/**
 * Hook used to load the default set of options for a selector dropdown
 * by using an aggregation query to the backend.
 *
 * TODO: Determine if we need to provide an option context that would help with filtering.
 */
export const useLoadAggregationOptions = (field: FilterField, visible: boolean, includeCounts: boolean) => {
    const [options, setOptions] = useState<FilterValueOption[]>([]);

    const [aggregateAcrossEntities, { loading }] = useAggregateAcrossEntitiesLazyQuery({
        onCompleted: (result) => setOptions(mapAggregateAcrossEntitiesOptions(field, result, includeCounts)),
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (visible) {
            aggregateAcrossEntities({
                variables: {
                    input: {
                        query: '*',
                        facets: [field.field],
                        searchFlags: {
                            maxAggValues: MAX_AGGREGATION_COUNT,
                        },
                    },
                },
            });
        }
    }, [visible, field.field, aggregateAcrossEntities]);

    if (!visible) {
        return { loading: false, options: [] };
    }

    return { options, loading };
};

/**
 * Hook used to load autocomplete / search options when a user types into a selector dropdown
 * search bar.
 */
export const useLoadSearchOptions = (field: FilterField, query?: string, skip?: boolean) => {
    const [options, setOptions] = useState<FilterValueOption[]>([]);

    const [searchAcrossEntities, { loading }] = useGetSearchResultsForMultipleLazyQuery({
        onCompleted: (result) => setOptions(mapAutoCompleteOptions(field, result)),
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (query && !skip) {
            searchAcrossEntities({
                variables: {
                    input: {
                        query,
                        types: field.entityTypes,
                        start: 0,
                        count: 20,
                    },
                },
            });
        }
    }, [query, skip, field.entityTypes, searchAcrossEntities]);

    if (!query || skip) {
        return { loading: false, options: [] };
    }

    return { options, loading };
};

/**
 * Hook used to filter selector options by a raw search query string. This uses the name of the filter value
 * to match against the search query.
 */
export const useFilterOptionsBySearchQuery = (
    options: FilterValueOption[],
    searchQuery?: string,
): FilterValueOption[] => {
    const entityRegistry = useEntityRegistry();
    if (!searchQuery) {
        return options;
    }
    return options.filter((option) => {
        const optionName = option.entity
            ? entityRegistry.getDisplayName(option.entity.type, option.entity)
            : option.value;
        return filterOptionsWithSearch(searchQuery, optionName, []);
    });
};

export const getEntityTypeFilterValueDisplayName = (value: string, entityRegistry: EntityRegistry) => {
    if (value.includes(FILTER_DELIMITER)) {
        const subType = value.split(FILTER_DELIMITER)[1];
        return capitalizeFirstLetterOnly(subType);
    }
    return entityRegistry.getCollectionName(value.toUpperCase() as EntityType);
};

export const getDefaultFieldOperatorType = (field: FilterField) => {
    return field.type === FieldType.TEXT ? FilterOperatorType.CONTAINS : FilterOperatorType.EQUALS;
};
