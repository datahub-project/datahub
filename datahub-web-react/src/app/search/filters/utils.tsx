import moment from 'moment-timezone';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import Icon from '@ant-design/icons/lib/components/Icon';
import TableIcon from '@src/images/table-icon.svg?react';
import {
    BookOutlined,
    DatabaseOutlined,
    FileOutlined,
    FolderFilled,
    FolderOutlined,
    TagOutlined,
    UserOutlined,
} from '@ant-design/icons';
import { removeMarkdown } from '@src/app/entity/shared/components/styled/StripMarkdownText';
import { DATE_TYPE_URN } from '@src/app/shared/constants';
import React, { useLayoutEffect, useState } from 'react';
import styled from 'styled-components';
import {
    AggregationMetadata,
    DataPlatform,
    DataPlatformInstance,
    Domain,
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    GlossaryTerm,
    Container,
    StructuredPropertyEntity,
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
    STRUCTURED_PROPERTIES_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UNIT_SEPARATOR,
} from '../utils/constants';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import DomainsIcon from '../../../images/domain.svg?react';
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
    facetEntity?: Entity | null,
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
    } else if (filterField === CONTAINER_FILTER_NAME) {
        // Scenario where the filter entity exists and filterField is container
        const logoUrl = (filterEntity as Container)?.platform?.properties?.logoUrl;
        icon = logoUrl ? (
            <PlatformIcon src={logoUrl} size={size} />
        ) : (
            entityRegistry.getIcon(EntityType.DataPlatform, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9])
        );
        label = entityRegistry.getDisplayName(EntityType.Container, filterEntity);
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
    } else if (filterField.startsWith(STRUCTURED_PROPERTIES_FILTER_NAME)) {
        label = getStructuredPropFilterDisplayName(filterField, filterValue, facetEntity);
        icon = <Icon component={TableIcon} />;
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
    if (field.startsWith(STRUCTURED_PROPERTIES_FILTER_NAME)) {
        return <Icon component={TableIcon} />;
    }

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
    filterEntity?: Entity | null,
) {
    const aggregationFilterOptions = aggregations.map((agg) => ({
        field: filterField,
        displayName: getStructuredPropFilterDisplayName(filterField, agg.value, filterEntity),
        ...agg,
    }));

    const searchResults = autoCompleteResults?.autoCompleteForMultiple?.suggestions?.find((suggestion) =>
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

export function getParentEntities(entity: Entity): Entity[] | null {
    if (!entity) {
        return null;
    }
    if (entity.type === EntityType.GlossaryTerm) {
        return (entity as GlossaryTerm).parentNodes?.nodes || [];
    }
    if (entity.type === EntityType.Domain) {
        return (entity as Domain).parentDomains?.domains || [];
    }
    if (entity.type === EntityType.Container) {
        return (entity as Container).parentContainers?.containers || [];
    }
    return null;
}

/**
 * Utility function to get the dimensions of a DOM element.
 * @param {React.MutableRefObject<HTMLElement | null>} ref - Reference to the DOM element.
 * @returns {Object} - Object containing width and height of the element.
 */
export function useElementDimensions(ref) {
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

    useLayoutEffect(() => {
        const updateDimensions = () => {
            if (ref.current) {
                setDimensions({
                    width: ref.current.offsetWidth,
                    height: ref.current.offsetHeight,
                });
            }
        };

        updateDimensions();
    }, [ref]);

    return dimensions;
}

export function getStructuredPropFilterDisplayName(field: string, value: string, entity?: Entity | null) {
    const isStructuredPropertyValue = field.startsWith('structuredProperties.');
    if (!isStructuredPropertyValue) return undefined;

    // check for structured prop entity values
    if (value.startsWith('urn:li:')) {
        // this value is an urn, handle entity display names elsewhere
        return undefined;
    }

    // check for structured prop date values
    if (entity && (entity as StructuredPropertyEntity).definition?.valueType?.urn === DATE_TYPE_URN) {
        return moment(parseInt(value, 10)).tz('GMT').format('MM/DD/YYYY').toString();
    }

    // check for structured prop number values
    if (!Number.isNaN(parseFloat(value))) {
        return parseFloat(value).toString();
    }

    return removeMarkdown(value);
}

export function useFilterDisplayName(filter: FacetMetadata, predicateDisplayName?: string) {
    const entityRegistry = useEntityRegistry();

    if (filter.entity) {
        return entityRegistry.getDisplayName(filter.entity.type, filter.entity);
    }

    return predicateDisplayName || filter.displayName || filter.field;
}

export function getIsDateRangeFilter(field: FacetMetadata) {
    if (field.entity && field.entity.type === EntityType.StructuredProperty) {
        return (field.entity as StructuredPropertyEntity).definition?.valueType?.urn === DATE_TYPE_URN;
    }
    return false;
}
