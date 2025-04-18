import { AggregationMetadata, FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { EntityRegistry } from '../../../entityRegistryContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getFilterIconAndLabel } from '../filters/utils';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_FIELDS,
    PLATFORM_FILTER_NAME,
} from '../../search/utils/constants';
import { RecommendedFilter } from './types';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER } from '../utils/constants';

// Maximum number of recommended filters to show.
const MAX_FILTERS = 6;

const toRecommendedFilter = (
    field: string,
    aggregation: AggregationMetadata,
    entityRegistry: EntityRegistry,
): RecommendedFilter => {
    const { icon, label } = getFilterIconAndLabel(
        field,
        aggregation.value,
        entityRegistry,
        aggregation.entity ?? null,
        14,
    );
    return {
        field,
        value: aggregation.value,
        label,
        icon,
    };
};

/**
 * Strategy for recommending search filters:
 * If there is no platform filter selected, recommend the top 3 platforms.
 * If there is a platform filter selected, recommend the top 3 sub types. (If not, fall back to entity types).
 * If there is a sub type filter selected, recommend the top 3 domains.
 */
export const useGetRecommendedFilters = (
    availableFilters: FacetMetadata[],
    selectedFilters: FacetFilterInput[],
): RecommendedFilter[] => {
    const entityRegistry = useEntityRegistry();
    const isPlatformFilterApplied = selectedFilters.find((filter) => filter.field === PLATFORM_FILTER_NAME);
    const isTypeFilterApplied = selectedFilters.find((filter) => ENTITY_SUB_TYPE_FILTER_FIELDS.includes(filter.field));
    const isDomainFilterApplied = selectedFilters.find((filter) => filter.field === DOMAINS_FILTER_NAME);

    const platformAggregations: AggregationMetadata[] =
        availableFilters.find((filter) => filter.field === PLATFORM_FILTER_NAME)?.aggregations || [];
    const subTypeAggregations: AggregationMetadata[] =
        availableFilters
            .find((filter) => filter.field === ENTITY_SUB_TYPE_FILTER_NAME)
            ?.aggregations?.filter((agg) => agg.value.split(FILTER_DELIMITER).length > 1) || [];
    const entityTypeAggregations: AggregationMetadata[] =
        availableFilters.find((filter) => filter.field === ENTITY_FILTER_NAME)?.aggregations || [];
    const domainAggregations: AggregationMetadata[] =
        availableFilters.find((filter) => filter.field === DOMAINS_FILTER_NAME)?.aggregations || [];

    const recommendedFilters: RecommendedFilter[] = [];

    if (!isPlatformFilterApplied && platformAggregations?.length > 1) {
        recommendedFilters.push(
            ...platformAggregations
                .filter((agg) => agg.count > 0)
                .slice(0, MAX_FILTERS)
                .map((agg) => toRecommendedFilter(PLATFORM_FILTER_NAME, agg, entityRegistry)),
        );
    } else if (!isTypeFilterApplied) {
        if (subTypeAggregations?.length > 1) {
            recommendedFilters.push(
                ...subTypeAggregations
                    .filter((agg) => agg.count > 0)
                    .slice(0, MAX_FILTERS)
                    .map((agg) => toRecommendedFilter(ENTITY_SUB_TYPE_FILTER_NAME, agg, entityRegistry)),
            );
        } else if (entityTypeAggregations?.length > 1) {
            recommendedFilters.push(
                ...entityTypeAggregations
                    .filter((agg) => agg.count > 0)
                    .slice(0, MAX_FILTERS - recommendedFilters.length)
                    .map((agg) => toRecommendedFilter(ENTITY_FILTER_NAME, agg, entityRegistry)),
            );
        }
    } else if (!isDomainFilterApplied && domainAggregations.length > 1) {
        recommendedFilters.push(
            ...domainAggregations
                .filter((agg) => agg.count > 0)
                .slice(0, MAX_FILTERS)
                .map((agg) => toRecommendedFilter(DOMAINS_FILTER_NAME, agg, entityRegistry)),
        );
    }

    return recommendedFilters;
};
