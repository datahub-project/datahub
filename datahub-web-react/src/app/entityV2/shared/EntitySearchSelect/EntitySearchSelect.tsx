import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
    Container,
    SelectBase,
    SelectLabel,
    SelectLabelContainer,
    StyledIcon,
} from '@components/components/Select/components';
import SelectActionButtons from '@components/components/Select/private/SelectActionButtons';
import SelectLabelRenderer from '@components/components/Select/private/SelectLabelRenderer/SelectLabelRenderer';
import { SelectOption, SelectSizeOptions } from '@components/components/Select/types';

import { EntitySearchDropdown } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchDropdown';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitiesLazyQuery } from '@graphql/entity.generated';
import { Entity, EntityType } from '@types';

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

const addToCache = (cache: Map<string, Entity>, entity: Entity) => {
    const newCache = new Map(cache);
    newCache.set(entity.urn, entity);
    return newCache;
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
    const [isOpen, setIsOpen] = useState(false);
    const selectRef = useRef<HTMLDivElement>(null);

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();
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

    const handleSelectionChange = useCallback(
        (newUrns: string[]) => {
            if (!isMultiSelect && newUrns.length > 0) {
                // Single select - close dropdown immediately
                setIsOpen(false);
            }
            onUpdate?.(newUrns);
        },
        [isMultiSelect, onUpdate],
    );

    const handleEntitySelect = useCallback((entity: Entity) => {
        // Add entity to cache when selected
        setEntityCache((prevCache) => addToCache(prevCache, entity));
    }, []);

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

    const selectBase = (
        <SelectBase
            isDisabled={isDisabled}
            isReadOnly={isReadOnly}
            isRequired={isRequired}
            isOpen={isOpen}
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
    );

    const handleOpenChange = useCallback(
        (open: boolean) => {
            if (!isDisabled && !isReadOnly) {
                setIsOpen(open);
            }
        },
        [isDisabled, isReadOnly],
    );

    return (
        <Container ref={selectRef} size={size} width={width}>
            {label && (
                <SelectLabel
                    onClick={() => {
                        if (!isDisabled && !isReadOnly) {
                            setIsOpen((prev) => !prev);
                        }
                    }}
                >
                    {label}
                </SelectLabel>
            )}
            <EntitySearchDropdown
                entityTypes={entityTypes}
                selectedUrns={selectedUrns}
                onSelectionChange={handleSelectionChange}
                placeholder="Search..."
                isMultiSelect={isMultiSelect}
                onEntitySelect={handleEntitySelect}
                trigger={selectBase}
                open={isOpen}
                onOpenChange={handleOpenChange}
                placement="bottomRight"
                disabled={isDisabled}
            />
        </Container>
    );
};
