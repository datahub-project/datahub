import moment from 'moment-timezone';
import { removeMarkdown } from '@src/app/entity/shared/components/styled/StripMarkdownText';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { FolderFilled } from '@ant-design/icons';
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
    FilterOperator,
    GlossaryTerm,
    StructuredPropertyEntity,
    Tag,
} from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import {
    BOOLEAN_FIELDS,
    BROWSE_PATH_V2_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FIELDS,
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    FILTER_DELIMITER,
    GLOSSARY_TERMS_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TEXT_FIELDS,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UNIT_SEPARATOR,
    ENTITY_TYPE_FIELDS,
    DATA_PLATFORM_INSTANCE_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_TAGS_FILTER_NAME,
    PROPOSED_SCHEMA_TAGS_FILTER_NAME,
    PROPOSED_GLOSSARY_TERMS_FILTER_NAME,
    PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME,
    LAST_MODIFIED_FILTER_NAME,
    STRUCTURED_PROPERTIES_FILTER_NAME,
} from '../utils/constants';
import { EntityRegistry } from '../../../entityRegistryContext';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { GetAutoCompleteMultipleResultsQuery } from '../../../graphql/search.generated';
import { FACETS_TO_ENTITY_TYPES } from './constants';
import { FieldType, FilterField, FilterOperatorType, FilterOptionType, FilterPredicate } from './types';
import { capitalizeFirstLetterOnly, forcePluralize, pluralizeIfIrregular } from '../../shared/textUtil';
import { convertBackendToFrontendOperatorType } from './operator/operator';
import { ALL_FILTER_FIELDS, STRUCTURED_PROPERTY_FILTER } from './field/fields';
import { getSubTypeIcon } from '../../entityV2/shared/components/subtypes';
import getTypeIcon from '../../sharedV2/icons/getTypeIcon';
import { DomainColoredIcon } from '../../entityV2/shared/links/DomainColoredIcon';
import { TagColor } from './FilterOption';

// either adds or removes selectedFilterValues to/from activeFilters for a given filterField
export function getNewFilters(
    filterField: string,
    activeFilters: FacetFilterInput[],
    selectedFilterValues: string[],
): FacetFilterInput[] {
    if (activeFilters.find((activeFilter) => activeFilter.field === filterField)) {
        return activeFilters
            .map((f) => (f.field === filterField ? { ...f, values: selectedFilterValues } : f))
            .filter((f) => !(f.values?.length === 0));
    }

    return [
        ...activeFilters,
        {
            field: filterField,
            values: selectedFilterValues,
            // TODO: Define on filter field instead
            condition: filterField === LAST_MODIFIED_FILTER_NAME ? FilterOperator.GreaterThan : undefined,
        },
    ].filter((f) => !(f.values?.length === 0));
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
    let label: string | null = null;
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

const SubTypeIcon = styled.span<{ $fontSize?: number }>`
    display: inline-flex;
    color: ${ANTD_GRAY[9]};
    font-size: ${({ $fontSize }) => $fontSize || 12}px;
`;

function getEntitySubtypeFilterIconAndLabel(filterValue: string, entityRegistry: EntityRegistry, size?: number) {
    let icon: React.ReactNode;
    let label: string | undefined;

    // If this includes a delimiter, it is a subType
    if (filterValue.includes(FILTER_DELIMITER)) {
        const [type, subType] = filterValue.split(FILTER_DELIMITER);
        label = capitalizeFirstLetterOnly(pluralizeIfIrregular(subType));
        icon = <SubTypeIcon $fontSize={size}>{getTypeIcon(entityRegistry, type as EntityType, subType)}</SubTypeIcon>;
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
    let label: string | undefined;

    if (filterEntity.type === EntityType.DataPlatformInstance) {
        const { icon: newIcon, label: newLabel } = getDataPlatformInstanceIconAndLabel(
            filterEntity,
            entityRegistry,
            size,
        );
        icon = newIcon;
        label = newLabel;
    } else if (entityRegistry.hasEntity(filterEntity.type)) {
        icon = entityRegistry.getIcon(filterEntity.type, size || 12, IconStyleType.ACCENT, ANTD_GRAY[9]);
        label = entityRegistry.getDisplayName(filterEntity.type, filterEntity);
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
    let label: string | undefined;

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
    } else if (filterField === TYPE_NAMES_FILTER_NAME) {
        icon = getSubTypeIcon(filterValue);
        label = capitalizeFirstLetterOnly(forcePluralize(filterValue));
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
    if (field.startsWith(STRUCTURED_PROPERTIES_FILTER_NAME)) {
        return STRUCTURED_PROPERTY_FILTER.icon;
    }
    return ALL_FILTER_FIELDS.find((filterField) => filterField.field === field)?.icon;
}

// maps aggregations and auto complete results to FilterOptionType[] and adds selected filter options if not already there
export function getFilterOptions(
    filterField: string,
    aggregations: AggregationMetadata[],
    selectedFilterOptions: FilterOptionType[],
    autoCompleteResults?: GetAutoCompleteMultipleResultsQuery,
) {
    const aggregationFilterOptions = aggregations.map((agg) => ({ field: filterField, ...agg }));

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
    const nestedSubTypeFilter = activeFilters?.find((f) => f.field === ENTITY_SUB_TYPE_FILTER_NAME);
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
    if (entity.type === EntityType.GlossaryTerm || entity.type === EntityType.GlossaryNode) {
        return (entity as GlossaryTerm).parentNodes?.nodes || [];
    }
    if (entity.type === EntityType.Domain) {
        return (entity as Domain).parentDomains?.domains || [];
    }
    return null;
}

const getInferredFieldType = (_: string, aggregationMetadata: AggregationMetadata[]) => {
    const entityAggregation = aggregationMetadata.find((agg) => agg.entity);
    if (entityAggregation) {
        return FieldType.ENTITY;
    }
    return undefined;
};

const getFilterFieldType = (field: string, aggregationMetadata: AggregationMetadata[]) => {
    if (ENTITY_TYPE_FIELDS.has(field)) {
        return FieldType.ENTITY_TYPE;
    }
    if (BOOLEAN_FIELDS.has(field)) {
        return FieldType.BOOLEAN;
    }
    if (TEXT_FIELDS.has(field)) {
        return FieldType.TEXT;
    }
    if (ENTITY_FIELDS.has(field)) {
        return FieldType.ENTITY;
    }
    const inferredType = getInferredFieldType(field, aggregationMetadata);
    if (inferredType) {
        // Inference - If there is an entity, we infer it as an entity type field.
        return inferredType;
    }
    // Select one of a few options.
    return FieldType.ENUM;
};

const getInferredFilterEntityTypes = (aggregationMetadata?: AggregationMetadata[]) => {
    const entityTypes = new Set<EntityType>();
    aggregationMetadata?.forEach((agg) => {
        const maybeEntity = agg.entity;
        if (maybeEntity) {
            entityTypes.add(maybeEntity.type);
        }
    });
    return Array.from(entityTypes);
};

const getFilterEntityTypes = (field: string, aggregationMetadata?: AggregationMetadata[]): EntityType[] => {
    if (OWNERS_FILTER_NAME === field) {
        return [EntityType.CorpUser, EntityType.CorpGroup];
    }
    if (DATA_PLATFORM_INSTANCE_FILTER_NAME === field) {
        return [EntityType.DataPlatformInstance];
    }
    if (DOMAINS_FILTER_NAME === field) {
        return [EntityType.Domain];
    }
    if (CONTAINER_FILTER_NAME === field) {
        return [EntityType.Container];
    }
    if (PLATFORM_FILTER_NAME === field) {
        return [EntityType.DataPlatform];
    }
    if (
        TAGS_FILTER_NAME === field ||
        FIELD_TAGS_FILTER_NAME === field ||
        PROPOSED_TAGS_FILTER_NAME === field ||
        PROPOSED_SCHEMA_TAGS_FILTER_NAME === field
    ) {
        return [EntityType.Tag];
    }
    if (
        GLOSSARY_TERMS_FILTER_NAME === field ||
        FIELD_GLOSSARY_TERMS_FILTER_NAME === field ||
        PROPOSED_GLOSSARY_TERMS_FILTER_NAME === field ||
        PROPOSED_SCHEMA_GLOSSARY_TERMS_FILTER_NAME === field
    ) {
        return [EntityType.GlossaryTerm];
    }
    const inferredTypes = getInferredFilterEntityTypes(aggregationMetadata);
    if (inferredTypes) {
        // Inference - If there is an entity, we infer it as an entity type field.
        return inferredTypes;
    }
    return [];
};

function getKnownFilterField(field: string): FilterField | undefined {
    return ALL_FILTER_FIELDS.find((filterField) => filterField.field === field);
}

function getDynamicFilterField(field: string, availableFilters: FacetMetadata[]): FilterField {
    const associatedAvailableFilter = availableFilters?.find((availableFilter) => availableFilter.field === field);
    const filterDisplayName = associatedAvailableFilter?.displayName;
    const filterAggregations = availableFilters?.find(
        (availableFilter) => availableFilter.field === field,
    )?.aggregations;
    return {
        field,
        displayName: filterDisplayName || field,
        type: getFilterFieldType(field, filterAggregations || []),
        entityTypes: getFilterEntityTypes(field, filterAggregations),
        icon: getFilterDropdownIcon(field),
        entity: associatedAvailableFilter?.entity || undefined,
    };
}

function getFilterValues(filter: FacetFilterInput, availableFilters: FacetMetadata[]) {
    const filterAggregations = availableFilters?.find((availableFilter) =>
        availableFilter.field.includes(filter?.field),
    )?.aggregations;
    return (
        filter?.values?.map((value) => {
            // IMPORTANT CODE ALERT: This code assumes that the applied filter value has an aggregation returned as part of the available filters.
            const filterAggregation = filterAggregations?.find((agg) => agg.value === value);
            return {
                value,
                entity: filterAggregation?.entity || null,
                count: filterAggregation?.count,
                displayName: filterAggregation?.displayName || undefined,
            };
        }) || []
    );
}

function getDefaultFilterOptions(filter: FacetFilterInput, availableFilters: FacetMetadata[]) {
    const currentFilter = availableFilters?.find((availableFilter) => availableFilter.field.includes(filter?.field));
    const filterAggregations = currentFilter?.aggregations;
    return (
        filterAggregations?.map((agg) => {
            return {
                value: agg.value,
                entity: agg.entity || null,
                count: agg.count,
                displayName:
                    agg.displayName ||
                    getStructuredPropFilterDisplayName(filter.field, agg.value, currentFilter?.entity),
            };
        }) || []
    );
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

/**
 * Converts a FacetFilterInput to a FilterPredicate for easier handling.
 *
 * @param filter - The selected filter  object to be converted.
 * @param availableFilters - An array of available facet filters metadata.
 * @returns The resulting FilterPredicate.
 */
export function convertToFilterPredicate(filter: FacetFilterInput, availableFilters: FacetMetadata[]): FilterPredicate {
    // First, check whether this is a well-supported filter field.
    const field = getKnownFilterField(filter.field) || getDynamicFilterField(filter.field, availableFilters);
    const operator =
        (filter.condition &&
            convertBackendToFrontendOperatorType({
                operator: filter.condition,
                negated: filter.negated || false,
            })) ||
        FilterOperatorType.EQUALS;

    const values = getFilterValues(filter, availableFilters);
    const defaultOptions = getDefaultFilterOptions(filter, availableFilters);

    return {
        field,
        operator,
        values,
        defaultValueOptions: defaultOptions,
    };
}

/**
 * Converts an array of selected facet filters to an array of FilterPredicates based on available filters metadata.
 * Here, we employ strict checking for the field value to differentiate between '_entityType␞typeNames' (recommended filter)
 * and '_entityType' filters. This strict checking ensures complete decoupling of these filters,
 * opting for direct equality comparison over 'includes'.
 *
 * @param selectedFilters - The array of selected facet filters to be converted.
 * @param availableFilters - An array of available facet filters metadata.
 * @returns An array of resulting FilterPredicates.
 */
export const convertToAvailableFilterPredictes = (
    selectedFilters: FacetFilterInput[],
    availableFilters: FacetMetadata[],
): FilterPredicate[] => {
    return availableFilters.map((filter) => {
        const selectedFilterObj = selectedFilters.find((obj) => obj.field === filter.field);
        return convertToFilterPredicate(selectedFilterObj || filter, availableFilters);
    });
};

/**
 * Converts an array of selected facet filters to an array of FilterPredicates based on available filters metadata.
 *
 * @param selectedFilters - The array of selected facet filters to be converted.
 * @param availableFilters - An array of available facet filters metadata.
 * @returns An array of resulting FilterPredicates.
 */

export const convertToSelectedFilterPredictes = (
    selectedFilters: FacetFilterInput[],
    availableFilters: FacetMetadata[],
): FilterPredicate[] => {
    return selectedFilters.map((filter) => {
        return convertToFilterPredicate(filter, availableFilters);
    });
};

interface FilterEntityIconProps {
    field: string;
    entity: Entity | null | undefined;
    icon: React.ReactNode | null;
}

export const FilterEntityIcon: React.FC<FilterEntityIconProps> = ({ field, entity, icon }) => {
    switch (true) {
        case field === PLATFORM_FILTER_NAME && entity !== null:
            return <>{icon}</>;

        case field === TAGS_FILTER_NAME && entity?.type === EntityType.Tag:
            return <TagColor color={(entity as Tag).properties?.colorHex || ''} colorHash={entity?.urn} />;

        case field === DOMAINS_FILTER_NAME && entity?.type === EntityType.Domain:
            return <DomainColoredIcon domain={entity as Domain} size={28} />;

        default:
            return null; // default
    }
};

/**
 * Custom hook to track dimensions of a DOM element.
 * @param {object} ref - Reference to the DOM element.
 * @returns {object} - Object containing width, height, and whether the element is outside the window.
 */
export function useElementDimensions(ref) {
    const [dimensions, setDimensions] = useState({ width: 0, height: 0, isElementOutsideWindow: false });

    useLayoutEffect(() => {
        const updateDimensions = () => {
            if (ref.current) {
                const { offsetWidth, offsetHeight } = ref.current;
                // Using data-testid to locate the "More Filters" dropdown button accurately, considering z-index and styling complexities of antd.
                const dropdownButton = document.querySelector('[data-testid="more-filters-dropdown"]');

                if (dropdownButton) {
                    const { left } = dropdownButton.getBoundingClientRect();
                    const windowWidth = window.innerWidth;
                    const isElementOutsideWindow = left + offsetWidth * 2 > windowWidth;

                    setDimensions({
                        width: offsetWidth,
                        height: offsetHeight,
                        isElementOutsideWindow,
                    });
                }
            }
        };

        updateDimensions();

        const handleResize = () => {
            updateDimensions();
        };
        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, [ref]);

    return dimensions;
}

export function useFilterDisplayName(filter: FacetMetadata | FilterField, predicateDisplayName?: string) {
    const entityRegistry = useEntityRegistryV2();

    if (filter.entity) {
        return entityRegistry.getDisplayName(filter.entity.type, filter.entity);
    }

    return predicateDisplayName || filter.displayName || filter.field;
}

export function getIsDateRangeFilter(field: FilterField | FacetMetadata) {
    if (field.entity && field.entity.type === EntityType.StructuredProperty) {
        return (field.entity as StructuredPropertyEntity).definition?.valueType?.urn === DATE_TYPE_URN;
    }
    return false;
}
