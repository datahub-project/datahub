import { LoadingOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import Dropdown from '@components/components/Dropdown/Dropdown';
import { Input } from '@components/components/Input/Input';
import {
    Container,
    DropdownContainer,
    LabelContainer,
    OptionContainer,
    OptionLabel,
    OptionList,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledCheckbox,
    StyledIcon,
} from '@components/components/Select/components';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import useSelectDropdown from '@components/components/Select/private/hooks/useSelectDropdown';
import { SelectOption, SelectSizeOptions } from '@components/components/Select/types';

import EntitySearchInputResultV2 from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitiesLazyQuery } from '@graphql/entity.generated';
import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

const EmptyState = styled.div`
    padding: 16px 12px;
    text-align: center;
    color: #8c8c8c;
    font-style: italic;
`;

const LoadingState = styled.div`
    padding: 16px 12px;
    text-align: center;
    color: #8c8c8c;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
`;

const SearchInputContainer = styled.div`
    padding: 8px 12px;
    border-bottom: 1px solid #f0f0f0;
`;

const EntityOptionContainer = styled.div`
    width: 100%;
`;

export interface EntitySearchSelectProps {
    selectedUrns?: string[];
    entityTypes: EntityType[];
    placeholder?: string;
    size?: SelectSizeOptions;
    isMultiSelect?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    label?: string;
    width?: number | 'full' | 'fit-content';
    onUpdate?: (selectedUrns: string[]) => void;
    showClear?: boolean;
    isRequired?: boolean;
    icon?: any;
}

interface EntityOption extends SelectOption {
    entity: Entity;
}

const addToCache = (cache: Map<string, Entity>, entity: Entity) => {
    cache.set(entity.urn, entity);
    return cache;
};

const buildCache = (entities: Entity[]) => {
    const cache = new Map();
    entities.forEach((entity) => cache.set(entity.urn, entity));
    return cache;
};

const isResolutionRequired = (urns: string[], cache: Map<string, Entity>) => {
    const uncachedUrns = urns.filter((urn) => !cache.has(urn));
    return uncachedUrns.length > 0;
};

/**
 * A standardized entity search and selection component that allows users to search
 * for DataHub entities and select one or multiple entities. Built on top of the
 * Select component library infrastructure for consistency.
 */
export const EntitySearchSelect: React.FC<EntitySearchSelectProps> = ({
    selectedUrns = [],
    entityTypes,
    placeholder = 'Search for entities...',
    size = 'md',
    isMultiSelect = false,
    isDisabled = false,
    isReadOnly = false,
    label,
    width = 255,
    onUpdate,
    showClear = true,
    isRequired = false,
    icon,
}) => {
    const entityRegistry = useEntityRegistry();
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());
    const [searchQuery, setSearchQuery] = useState('');
    const selectRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const {
        isOpen,
        isVisible,
        close: closeDropdown,
        toggle: toggleDropdown,
    } = useSelectDropdown(false, selectRef, dropdownRef);

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData, loading: entitiesLoading }] = useGetEntitiesLazyQuery();
    useEffect(() => {
        if (isResolutionRequired(selectedUrns, entityCache)) {
            getEntities({ variables: { urns: selectedUrns } });
        }
    }, [selectedUrns, entityCache, getEntities]);

    /**
     * Build cache from resolved entities
     */
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildCache(entities));
        }
    }, [resolvedEntitiesData]);

    /**
     * Search functionality
     */
    const [searchResources, { data: resourcesSearchData, loading: searchLoading }] =
        useGetSearchResultsForMultipleLazyQuery();

    const entityOptions: EntityOption[] = useMemo(() => {
        const results = resourcesSearchData?.searchAcrossEntities?.searchResults || [];
        return results.map((result) => ({
            label: entityRegistry.getDisplayName(result.entity.type, result.entity),
            value: result.entity.urn,
            entity: result.entity as Entity,
        }));
    }, [resourcesSearchData, entityRegistry]);

    const handleSelectClick = useCallback(() => {
        if (!isDisabled && !isReadOnly) {
            toggleDropdown();
        }
    }, [toggleDropdown, isDisabled, isReadOnly]);

    const handleOptionClick = useCallback(
        (option: EntityOption) => {
            // Add entity to cache
            setEntityCache(addToCache(entityCache, option.entity));

            let newUrns: string[];
            if (isMultiSelect) {
                newUrns = selectedUrns.includes(option.value)
                    ? selectedUrns.filter((urn) => urn !== option.value)
                    : [...selectedUrns, option.value];
            } else {
                newUrns = [option.value];
                closeDropdown();
            }

            onUpdate?.(newUrns);
        },
        [selectedUrns, isMultiSelect, onUpdate, closeDropdown, entityCache],
    );

    const handleSearchChange = useCallback(
        (value: string) => {
            setSearchQuery(value);
            searchResources({
                variables: {
                    input: {
                        types: entityTypes,
                        query: value || '*',
                        start: 0,
                        count: 10,
                    },
                },
            });
        },
        [entityTypes, searchResources],
    );

    const handleClearSelection = useCallback(() => {
        onUpdate?.([]);
    }, [onUpdate]);

    const removeOption = useCallback(
        (option: SelectOption) => {
            const newUrns = selectedUrns.filter((urn) => urn !== option.value);
            onUpdate?.(newUrns);
        },
        [selectedUrns, onUpdate],
    );

    /**
     * Issue a default search on component mount
     */
    useEffect(() => {
        searchResources({
            variables: {
                input: {
                    types: entityTypes,
                    query: '*',
                    start: 0,
                    count: 10,
                },
            },
        });
    }, [entityTypes, searchResources]);

    // Create options for selected values from cache
    const selectedOptions: SelectOption[] = useMemo(() => {
        return selectedUrns.map((urn) => {
            const entity = entityCache.get(urn);
            return {
                label: entity ? entityRegistry.getDisplayName(entity.type, entity) : urn,
                value: urn,
            };
        });
    }, [selectedUrns, entityCache, entityRegistry]);

    const isLoading = entitiesLoading || searchLoading;

    return (
        <Container ref={selectRef} size={size} width={width}>
            {label && <SelectLabel onClick={handleSelectClick}>{label}</SelectLabel>}
            {isVisible && (
                <Dropdown
                    open={isOpen}
                    disabled={isDisabled}
                    placement="bottomRight"
                    dropdownRender={() => (
                        <DropdownContainer ref={dropdownRef}>
                            <SearchInputContainer>
                                <Input
                                    label=""
                                    value={searchQuery}
                                    setValue={(value) => {
                                        const newValue = typeof value === 'function' ? value(searchQuery) : value;
                                        handleSearchChange(newValue);
                                    }}
                                    placeholder="Search..."
                                    icon={{ icon: 'Search' }}
                                    data-testid="entity-search-select-input"
                                />
                            </SearchInputContainer>
                            <OptionList>
                                {isLoading && (
                                    <LoadingState>
                                        <LoadingOutlined />
                                    </LoadingState>
                                )}
                                {!isLoading && entityOptions.length === 0 && <EmptyState>No entities found</EmptyState>}
                                {!isLoading &&
                                    entityOptions.map((option) => (
                                        <OptionLabel
                                            key={option.value}
                                            onClick={() => !isMultiSelect && handleOptionClick(option)}
                                            isSelected={selectedUrns.includes(option.value)}
                                            isMultiSelect={isMultiSelect}
                                            data-testid={`entity-search-option-${option.entity.urn.split(':').pop()}`}
                                        >
                                            {isMultiSelect ? (
                                                <LabelContainer>
                                                    <EntityOptionContainer>
                                                        <EntitySearchInputResultV2 entity={option.entity} />
                                                    </EntityOptionContainer>
                                                    <StyledCheckbox
                                                        onClick={() => handleOptionClick(option)}
                                                        checked={selectedUrns.includes(option.value)}
                                                    />
                                                </LabelContainer>
                                            ) : (
                                                <OptionContainer>
                                                    <EntitySearchInputResultV2 entity={option.entity} />
                                                </OptionContainer>
                                            )}
                                        </OptionLabel>
                                    ))}
                            </OptionList>
                        </DropdownContainer>
                    )}
                >
                    <SelectBase
                        isDisabled={isDisabled}
                        isReadOnly={isReadOnly}
                        isRequired={isRequired}
                        isOpen={isOpen}
                        onClick={handleSelectClick}
                        fontSize={size}
                        width={width}
                        data-testid="entity-search-select-select"
                    >
                        <SelectLabelContainer>
                            {icon && <StyledIcon icon={icon} size="lg" />}
                            <SelectLabelRenderer
                                selectedValues={selectedUrns}
                                options={selectedOptions}
                                placeholder={placeholder}
                                isMultiSelect={isMultiSelect}
                                removeOption={removeOption}
                                disabledValues={[]}
                                showDescriptions={false}
                            />
                        </SelectLabelContainer>
                        <SelectActionButtons
                            hasSelectedValues={selectedUrns.length > 0}
                            isOpen={isOpen}
                            isDisabled={!!isDisabled}
                            isReadOnly={!!isReadOnly}
                            handleClearSelection={handleClearSelection}
                            fontSize={size}
                            showClear={!!showClear}
                        />
                    </SelectBase>
                </Dropdown>
            )}
        </Container>
    );
};
