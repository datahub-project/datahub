import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

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

import { extractTypeFromUrn } from '@app/entity/shared/utils';
import { EntitySearchDropdown } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchDropdown';
import { getUserFilters } from '@app/shared/userSearchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntitiesLazyQuery } from '@graphql/entity.generated';
import { useGetIngestionSourceNamesLazyQuery } from '@graphql/ingestion.generated';
import { Entity, EntityType } from '@types';

interface EntitySearchSelectProps {
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
    const { t } = useTranslation('entity.shared.selectors');
    const entityRegistry = useEntityRegistry();
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());
    const [isOpen, setIsOpen] = useState(false);
    const selectRef = useRef<HTMLDivElement>(null);

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();

    // Ingestion sources aren't in the entity registry and can't be resolved through the generic
    // entities() query — they get their own name lookup, mirroring the pre-refactor policy form.
    const ingestionSourceUrns = useMemo(
        () => selectedUrns.filter((urn) => extractTypeFromUrn(urn) === EntityType.IngestionSource),
        [selectedUrns],
    );
    const standardUrns = useMemo(
        () => selectedUrns.filter((urn) => extractTypeFromUrn(urn) !== EntityType.IngestionSource),
        [selectedUrns],
    );

    const [getIngestionSourceNames, { data: sourceNamesData }] = useGetIngestionSourceNamesLazyQuery();

    const ingestionSourceNames = useMemo(() => {
        const sources = sourceNamesData?.listIngestionSources?.ingestionSources || [];
        return new Map(sources.map((source): [string, string] => [source.urn, source.name ?? source.urn]));
    }, [sourceNamesData]);

    useEffect(() => {
        if (isResolutionRequired(standardUrns, entityCache)) {
            getEntities({ variables: { urns: standardUrns } });
        }
    }, [standardUrns, entityCache, getEntities]);

    useEffect(() => {
        const unresolved = ingestionSourceUrns.filter((urn) => !entityCache.has(urn) && !ingestionSourceNames.has(urn));
        if (unresolved.length > 0) {
            getIngestionSourceNames({ variables: { urns: ingestionSourceUrns } });
        }
    }, [ingestionSourceUrns, entityCache, ingestionSourceNames, getIngestionSourceNames]);

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

    // Apply user filters when searching for CorpUser entities
    const defaultFilters = useMemo(() => {
        if (entityTypes.includes(EntityType.CorpUser)) {
            return getUserFilters();
        }
        return undefined;
    }, [entityTypes]);

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

            // Ingestion sources: the registry has no entry for them, so getDisplayName
            // would return an empty label. Use the cached pseudo-entity's name (set by the
            // dropdown on selection) or the dedicated name lookup instead.
            if (extractTypeFromUrn(urn) === EntityType.IngestionSource) {
                // The dropdown caches ingestion sources as pseudo-entities carrying a `name`,
                // which the Entity union itself doesn't declare.
                const cachedName = (entity as { name?: string } | undefined)?.name;
                return { label: cachedName || ingestionSourceNames.get(urn) || urn, value: urn };
            }

            return {
                label: entity ? entityRegistry.getDisplayName(entity.type, entity) : urn,
                value: urn,
            };
        });
    }, [selectedUrns, entityCache, entityRegistry, ingestionSourceNames]);

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
                placeholder={t('entitySearch.placeholder')}
                isMultiSelect={isMultiSelect}
                onEntitySelect={handleEntitySelect}
                defaultFilters={defaultFilters}
                trigger={selectBase}
                open={isOpen}
                onOpenChange={handleOpenChange}
                placement="bottomRight"
                disabled={isDisabled}
            />
        </Container>
    );
};
