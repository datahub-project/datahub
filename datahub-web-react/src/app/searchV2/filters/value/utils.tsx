import { useMemo } from 'react';
import { ENTITY_FILTER_NAME } from '@src/app/search/utils/constants';
import useGetSearchQueryInputs from '@src/app/search/useGetSearchQueryInputs';
import { EntityRegistry } from '../../../../entityRegistryContext';
import {
    useAggregateAcrossEntitiesQuery,
    useGetAutoCompleteMultipleResultsQuery,
    useGetSearchResultsForMultipleQuery,
} from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { FILTER_DELIMITER } from '../../utils/constants';
import { EntityFilterField, FieldType, FilterField, FilterOperatorType, FilterValueOption } from '../types';
import { filterOptionsWithSearch, getStructuredPropFilterDisplayName } from '../utils';

const MAX_AGGREGATION_COUNT = 40;

/**
 * Simply removes the baseOptions from moreOptions parameters in case the same filter option
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
    const { entityFilters, query, orFilters, viewUrn } = useGetSearchQueryInputs(
        useMemo(() => [field.field], [field.field]),
    );
    const { data, loading } = useAggregateAcrossEntitiesQuery({
        skip: !visible,
        variables: {
            input: {
                query,
                facets: [field.field],
                searchFlags: {
                    maxAggValues: MAX_AGGREGATION_COUNT,
                },
                types: field.field === ENTITY_FILTER_NAME ? null : entityFilters,
                orFilters,
                viewUrn,
            },
        },
        fetchPolicy: 'cache-first',
    });

    if (!visible) {
        return { loading: false, options: [] };
    }

    const requestedAgg = data?.aggregateAcrossEntities?.facets?.find((facet: any) => facet.field === field.field);
    const options = requestedAgg?.aggregations?.map((aggregation): FilterValueOption => {
        return {
            value: aggregation.value,
            entity: aggregation.entity,
            icon: field.icon,
            count: includeCounts ? aggregation.count : undefined,
            displayName: getStructuredPropFilterDisplayName(field.field, aggregation.value, field.entity),
        };
    });
    return { options: options || [], loading };
};

/**
 * Hook used to load autocomplete / search options when a user types into a selector dropdown
 * search bar.
 */
export const useLoadSearchOptions = (field: EntityFilterField, query?: string, skip?: boolean) => {
    const { data, loading } = useGetAutoCompleteMultipleResultsQuery({
        skip: skip || !query,
        variables: {
            input: {
                query: query || '',
                types: field.entityTypes,
                limit: 20,
            },
        },
        fetchPolicy: 'cache-first',
    });

    // do initial search to get initial data to display
    const { data: searchData, loading: searchLoading } = useGetSearchResultsForMultipleQuery({
        skip: skip || !!query, // only do a search if not doing auto-complete,
        variables: {
            input: {
                query: '*',
                types: field.entityTypes,
                count: 10,
            },
        },
        fetchPolicy: 'cache-first',
    });

    if (skip) {
        return { loading: false, options: [] };
    }

    const options = data?.autoCompleteForMultiple?.suggestions
        ?.flatMap((suggestion) => suggestion.entities)
        .map((entity): FilterValueOption => {
            return {
                value: entity.urn,
                entity,
                icon: field.icon,
            };
        });
    const searchOptions = searchData?.searchAcrossEntities?.searchResults.map((result) => ({
        value: result.entity.urn,
        entity: result.entity,
        icon: field.icon,
    }));

    return { options: options || searchOptions || [], loading: loading || searchLoading };
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
            : option.displayName || option.value;
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
