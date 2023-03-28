import React from 'react';
import styled from 'styled-components';
import { DataPlatform, Entity, EntityType, FacetFilterInput, FacetMetadata, Maybe } from '../../../types.generated';
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
    return availableFilters
        ?.find((facet) => facet.field === filterField)
        ?.aggregations.find((agg) => agg.value === filterValue)?.entity;
}

const PlatformIcon = styled.img`
    max-height: 12px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export function getFilterIconAndLabel(
    filterField: string,
    filterValue: string,
    entityRegistry: EntityRegistry,
    filterEntity?: Maybe<Entity>,
) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;

    if (filterField === ENTITY_FILTER_NAME) {
        icon = entityRegistry.getIcon(filterValue as EntityType, 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getCollectionName(filterValue.toUpperCase() as EntityType);
    } else if (filterField === PLATFORM_FILTER_NAME) {
        const logoUrl = (filterEntity as DataPlatform)?.properties?.logoUrl;
        icon = logoUrl ? (
            <PlatformIcon src={logoUrl} />
        ) : (
            entityRegistry.getIcon(EntityType.DataPlatform, 12, IconStyleType.ACCENT, ANTD_GRAY[9])
        );
        label = entityRegistry.getDisplayName(EntityType.DataPlatform, filterEntity);
    } else if (filterEntity) {
        icon = entityRegistry.getIcon(filterEntity.type, 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getDisplayName(filterEntity.type, filterEntity);
    } else {
        label = filterValue;
    }

    return { icon, label };
}
