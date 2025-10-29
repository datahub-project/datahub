import {
    BookOutlined,
    DatabaseOutlined,
    FileOutlined,
    FolderFilled,
    FolderOutlined,
    TagOutlined,
    UserOutlined,
} from '@ant-design/icons';
import Icon from '@ant-design/icons/lib/components/Icon';
import moment from 'moment-timezone';
import React, { useLayoutEffect, useState } from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import EntityRegistry from '@app/entity/EntityRegistry';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { FACETS_TO_ENTITY_TYPES } from '@app/search/filters/constants';
import { FilterOptionType } from '@app/search/filters/types';
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
} from '@app/search/utils/constants';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { removeMarkdown } from '@src/app/entity/shared/components/styled/StripMarkdownText';
import { STRUCTURED_PROPERTY_FILTER } from '@src/app/searchV2/filters/field/fields';
import { DATE_TYPE_URN } from '@src/app/shared/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import TableIcon from '@src/images/table-icon.svg?react';

import { GetAutoCompleteMultipleResultsQuery } from '@graphql/search.generated';
import {
    AggregationMetadata,
    Container,
    DataPlatform,
    DataPlatformInstance,
    Domain,
    Entity,
    EntityType,
    FacetFilterInput,
    FacetMetadata,
    GlossaryTerm,
    StructuredPropertyEntity,
} from '@types';

import DomainsIcon from '@images/domain.svg?react';

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
    const logoUrl = (filterEntity as DataPlatformInstance)?.platform?.properties?.logoUrl;
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

function getNumActiveFiltersForFilter(activeFilters: FacetFilterInput[], filter: FacetMetadata) {
    return activeFilters.find((f) => f.field === filter.field)?.values?.length || 0;
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

function getStructuredPropFilterDisplayName(field: string, value: string, entity?: Entity | null) {
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
