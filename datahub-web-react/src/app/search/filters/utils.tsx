import React from 'react';
import styled from 'styled-components';
import {
    AggregationMetadata,
    DataPlatform,
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
} from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { ENTITY_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';

// either adds or removes selectedFilterValues to/from activeFilters for a given filterField
export function getNewFilters(filterField: string, activeFilters: FacetFilterInput[], selectedFilterValues: string[]) {
    let newFilters = activeFilters;
    if (activeFilters.find((activeFilter) => activeFilter.field === filterField)) {
        newFilters = activeFilters
            .map((f) => (f.field === filterField ? { ...f, values: selectedFilterValues } : f))
            .filter((f) => !(f.values?.length === 0));
    } else {
        newFilters = [...activeFilters, { field: filterField, values: selectedFilterValues }].filter(
            (f) => !(f.values?.length === 0),
        );
    }
    return newFilters;
}

export function isFilterOptionSelected(selectedFilterValues: string[], filterValue: string) {
    return !!selectedFilterValues.find((value) => value === filterValue);
}

export function getFilterEntity(filterField: string, filterValue: string, availableFilters: FacetMetadata[] | null) {
    return (
        availableFilters
            ?.find((facet) => facet.field === filterField)
            ?.aggregations.find((agg) => agg.value === filterValue)?.entity || null
    );
}

export const PlatformIcon = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 12)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export function getFilterIconAndLabel(
    filterField: string,
    filterValue: string,
    entityRegistry: EntityRegistry,
    filterEntity: Entity | null,
    size?: number,
) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;

    if (filterField === ENTITY_FILTER_NAME) {
        icon = entityRegistry.getIcon(filterValue as EntityType, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getCollectionName(filterValue.toUpperCase() as EntityType);
    } else if (filterField === PLATFORM_FILTER_NAME) {
        const logoUrl = (filterEntity as DataPlatform)?.properties?.logoUrl;
        icon = logoUrl ? (
            <PlatformIcon src={logoUrl} size={size} />
        ) : (
            entityRegistry.getIcon(EntityType.DataPlatform, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9])
        );
        label = entityRegistry.getDisplayName(EntityType.DataPlatform, filterEntity);
    } else if (filterEntity) {
        icon = entityRegistry.getIcon(filterEntity.type, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getDisplayName(filterEntity.type, filterEntity);
    } else {
        label = filterValue;
    }

    return { icon, label };
}

export function getNumActiveFiltersForFilter(activeFilters: FacetFilterInput[], filter: FacetMetadata) {
    return activeFilters.find((f) => f.field === filter.field)?.values?.length || 0;
}

export function getNumActiveFiltersForGroupOfFilters(activeFilters: FacetFilterInput[], filters: FacetMetadata[]) {
    let numActiveFilters = 0;
    filters.forEach((filter) => {
        numActiveFilters += getNumActiveFiltersForFilter(activeFilters, filter);
    });
    return numActiveFilters;
}

// combine existing aggs from search response with new aggregateAcrossEntities response
export function combineAggregations(
    filterField: string,
    originalAggs: AggregationMetadata[],
    newFacets?: FacetMetadata[] | null,
) {
    const combinedAggregations = [...originalAggs];
    if (newFacets) {
        const newAggregations = newFacets.find((facet) => facet.field === filterField)?.aggregations;
        newAggregations?.forEach((agg) => {
            // only add the new aggregations if it is not already in the original to avoid dupes
            if (!originalAggs.find((existingAgg) => existingAgg.value === agg.value)) {
                combinedAggregations.push(agg);
            }
        });
    }
    return combinedAggregations;
}

// filter out any aggregations with a count of 0 unless it's already selected and in activeFilters
export function filterEmptyAggregations(aggregations: AggregationMetadata[], activeFilters: FacetFilterInput[]) {
    return aggregations.filter((agg) => {
        if (agg.count === 0) {
            return activeFilters.find((activeFilter) => activeFilter.values?.includes(agg.value));
        }
        return true;
    });
}
