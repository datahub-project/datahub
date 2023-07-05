import Icon from '@ant-design/icons/lib/components/Icon';
import {
    BookOutlined,
    DatabaseOutlined,
    FileOutlined,
    FolderFilled,
    FolderOutlined,
    TagOutlined,
    UserOutlined,
} from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import {
    AggregationMetadata,
    DataPlatform,
    DataPlatformInstance,
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
} from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FILTER_DELIMITER,
    GLOSSARY_TERMS_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UNIT_SEPARATOR,
} from '../utils/constants';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { ReactComponent as DomainsIcon } from '../../../images/domain.svg';
import { GetAutoCompleteMultipleResultsQuery } from '../../../graphql/search.generated';
import { FACETS_TO_ENTITY_TYPES } from './constants';
import { FilterOptionType } from './types';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';

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

export function isFilterOptionSelected(selectedFilterOptions: FilterOptionType[], filterValue: string) {
    const parentFilterValues = filterValue.includes(FILTER_DELIMITER)
        ? filterValue.split(FILTER_DELIMITER).slice(0, -1)
        : [];
    return !!selectedFilterOptions.find(
        (option) => option.value === filterValue || parentFilterValues.includes(option.value),
    );
}

export function isAnyOptionSelected(selectedFilterOptions: FilterOptionType[], filterValues?: string[]) {
    return selectedFilterOptions.some((option) => filterValues?.some((filterValue) => filterValue === option.value));
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

function getDataPlatformInstanceIconAndLabel(
    filterEntity: Entity | null,
    entityRegistry: EntityRegistry,
    size?: number,
) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;
    const logoUrl = (filterEntity as DataPlatformInstance)?.platform.properties?.logoUrl;
    icon = logoUrl ? (
        <PlatformIcon src={logoUrl} size={size} />
    ) : (
        entityRegistry.getIcon(EntityType.DataPlatform, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9])
    );
    label = (filterEntity as DataPlatformInstance).instanceId
        ? (filterEntity as DataPlatformInstance).instanceId
        : (filterEntity as DataPlatformInstance).urn;

    return { icon, label };
}

export function getLastBrowseEntryFromFilterValue(filterValue: string) {
    const browseEntries = filterValue.split(UNIT_SEPARATOR);
    return browseEntries[browseEntries.length - 1] || '';
}

function getEntitySubtypeFilterIconAndLabel(filterValue: string, entityRegistry: EntityRegistry, size?: number) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;

    // If this includes a delimiter, it is a subType
    if (filterValue.includes(FILTER_DELIMITER)) {
        const subType = filterValue.split(FILTER_DELIMITER)[1];
        label = capitalizeFirstLetterOnly(subType);
    } else {
        icon = entityRegistry.getIcon(filterValue as EntityType, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getCollectionName(filterValue.toUpperCase() as EntityType);
    }

    return { icon, label };
}

function getFilterWithEntityIconAndLabel(
    filterValue: string,
    entityRegistry: EntityRegistry,
    filterEntity: Entity,
    size?: number,
) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;

    if (entityRegistry.hasEntity(filterEntity.type)) {
        icon = entityRegistry.getIcon(filterEntity.type, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getDisplayName(filterEntity.type, filterEntity);
    } else if (filterEntity.type === EntityType.DataPlatformInstance) {
        const { icon: newIcon, label: newLabel } = getDataPlatformInstanceIconAndLabel(
            filterEntity,
            entityRegistry,
            size,
        );
        icon = newIcon;
        label = newLabel;
    } else {
        label = filterValue;
    }

    return { icon, label };
}

export function getFilterIconAndLabel(
    filterField: string,
    filterValue: string,
    entityRegistry: EntityRegistry,
    filterEntity: Entity | null,
    size?: number,
    filterLabelOverride?: string | null,
) {
    let icon: React.ReactNode = null;
    let label: React.ReactNode = null;

    if (filterField === ENTITY_FILTER_NAME || filterField === LEGACY_ENTITY_FILTER_NAME) {
        icon = entityRegistry.getIcon(filterValue as EntityType, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getCollectionName(filterValue.toUpperCase() as EntityType);
    } else if (filterField === ENTITY_SUB_TYPE_FILTER_NAME) {
        const { icon: newIcon, label: newLabel } = getEntitySubtypeFilterIconAndLabel(
            filterValue,
            entityRegistry,
            size,
        );
        icon = newIcon;
        label = newLabel;
    } else if (filterField === PLATFORM_FILTER_NAME) {
        const logoUrl = (filterEntity as DataPlatform)?.properties?.logoUrl;
        icon = logoUrl ? (
            <PlatformIcon src={logoUrl} size={size} />
        ) : (
            entityRegistry.getIcon(EntityType.DataPlatform, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9])
        );
        label = filterEntity ? entityRegistry.getDisplayName(EntityType.DataPlatform, filterEntity) : filterValue;
    } else if (filterField === BROWSE_PATH_V2_FILTER_NAME) {
        icon = <FolderFilled size={size} color="black" />;
        label = getLastBrowseEntryFromFilterValue(filterValue);
    } else if (filterEntity) {
        const { icon: newIcon, label: newLabel } = getFilterWithEntityIconAndLabel(
            filterValue,
            entityRegistry,
            filterEntity,
            size,
        );
        icon = newIcon;
        label = newLabel;
    } else {
        label = filterValue;
    }

    if (filterLabelOverride) {
        label = filterLabelOverride;
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

export function sortFacets(facetA: FacetMetadata, facetB: FacetMetadata, sortedFacetFields: string[]) {
    if (sortedFacetFields.indexOf(facetA.field) === -1) return 1;
    if (sortedFacetFields.indexOf(facetB.field) === -1) return -1;
    return sortedFacetFields.indexOf(facetA.field) - sortedFacetFields.indexOf(facetB.field);
}

export function getFilterDropdownIcon(field: string) {
    switch (field) {
        case PLATFORM_FILTER_NAME:
            return <DatabaseOutlined />;
        case ENTITY_FILTER_NAME:
            return <FileOutlined />;
        case TYPE_NAMES_FILTER_NAME:
            return <FileOutlined />;
        case GLOSSARY_TERMS_FILTER_NAME:
            return <BookOutlined />;
        case TAGS_FILTER_NAME:
            return <TagOutlined />;
        case OWNERS_FILTER_NAME:
            return <UserOutlined />;
        case CONTAINER_FILTER_NAME:
            return <FolderOutlined />;
        case DOMAINS_FILTER_NAME:
            return <Icon component={DomainsIcon} />;
        default:
            return null;
    }
}

// maps aggregations and auto complete results to FilterOptionType[] and adds selected filter options if not already there
export function getFilterOptions(
    filterField: string,
    aggregations: AggregationMetadata[],
    selectedFilterOptions: FilterOptionType[],
    autoCompleteResults?: GetAutoCompleteMultipleResultsQuery,
) {
    const aggregationFilterOptions = aggregations.map((agg) => ({ field: filterField, ...agg }));

    const searchResults = autoCompleteResults?.autoCompleteForMultiple?.suggestions.find((suggestion) =>
        FACETS_TO_ENTITY_TYPES[filterField]?.includes(suggestion.type),
    );
    const searchFilterOptions = searchResults?.entities
        .filter((entity) => !aggregations.find((agg) => agg.value === entity.urn))
        .map((entity) => ({ field: filterField, value: entity.urn, entity }));

    let filterOptions: FilterOptionType[] = searchFilterOptions
        ? [...aggregationFilterOptions, ...searchFilterOptions]
        : aggregationFilterOptions;

    // if a selected filter option is not in this list (because search results have changed) then insert at the beginning of the list
    selectedFilterOptions.forEach((selectedOption) => {
        if (!filterOptions.find((option) => option.value === selectedOption.value)) {
            filterOptions = [selectedOption, ...filterOptions];
        }
    });

    return filterOptions;
}

export function filterOptionsWithSearch(searchQuery: string, name: string, nestedOptions: FilterOptionType[] = []) {
    if (searchQuery) {
        return (
            name.toLocaleLowerCase().includes(searchQuery.toLocaleLowerCase()) ||
            !!nestedOptions.find((option) => option.value.toLocaleLowerCase().includes(searchQuery.toLocaleLowerCase()))
        );
    }
    return true;
}

export const createBrowseV2SearchFilter = (path: Array<string>) => `${UNIT_SEPARATOR}${path.join(UNIT_SEPARATOR)}`;

export function canCreateViewFromFilters(activeFilters: FacetFilterInput[]) {
    const nestedSubTypeFilter = activeFilters.find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME);
    if (nestedSubTypeFilter) {
        const entityTypes = nestedSubTypeFilter.values?.filter((value) => !value.includes(FILTER_DELIMITER));
        const subTypes = nestedSubTypeFilter.values?.filter((value) => value.includes(FILTER_DELIMITER));

        if (entityTypes?.length && subTypes?.length) {
            return false;
        }
    }
    return true;
}
