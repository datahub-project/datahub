import { SelectOption } from '@src/alchemy-components/components/Select/types';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, OwnerEntityType, OwnershipType, EntityType } from '@types';

/**
 * Entity caching utilities
 */
export const buildEntityCache = (entities: Entity[]) => {
    const cache = new Map<string, Entity>();
    entities.forEach((entity) => cache.set(entity.urn, entity));
    return cache;
};

export const isEntityResolutionRequired = (urns: string[], entityCache: Map<string, Entity>) => {
    const uncachedUrns = urns.filter((urn) => !entityCache.has(urn));
    return uncachedUrns.length > 0;
};

/**
 * Entity deduplication utilities
 */
export interface DeduplicateEntitiesOptions {
    placeholderEntities?: Entity[];
    searchResults?: Entity[];
    selectedEntities?: Entity[];
    existingEntityUrns?: string[];
}

export const deduplicateEntities = ({
    placeholderEntities = [],
    searchResults = [],
    selectedEntities = [],
    existingEntityUrns = [],
}: DeduplicateEntitiesOptions): Entity[] => {
    // Combine all entity sources
    const allEntitySources = [
        ...placeholderEntities,
        ...searchResults,
        ...selectedEntities,
    ];

    // Deduplicate by URN and exclude existing entities
    // BUT always include selected entities (they must remain in options for removal)
    const existingUrnsSet = new Set(existingEntityUrns);
    const selectedUrnsSet = new Set(selectedEntities.map(e => e.urn));
    const seenUrns = new Set<string>();
    
    return allEntitySources.filter((entity) => {
        // Skip if already seen, unless it's a selected entity that must be included
        if (seenUrns.has(entity.urn)) {
            return false;
        }
        
        // Skip existing entities (but not selected ones)
        if (existingUrnsSet.has(entity.urn) && !selectedUrnsSet.has(entity.urn)) {
            return false;
        }
        
        seenUrns.add(entity.urn);
        return true;
    });
};

/**
 * Convert entities to SelectOption format
 */
export const entitiesToSelectOptions = (entities: Entity[], entityRegistry: ReturnType<typeof useEntityRegistryV2>): SelectOption[] => {
    return entities.map((entity) => ({
        value: entity.urn,
        label: entityRegistry.getDisplayName(entity.type, entity),
        description: entity.type === EntityType.CorpUser ? (entity as any)?.properties?.email : undefined,
    }));
};

/**
 * Convert entities to NestedSelectOption format with fallback labels
 */
export const entitiesToNestedSelectOptions = (
    entityUrns: string[], 
    entityCache: Map<string, Entity>, 
    entityRegistry: ReturnType<typeof useEntityRegistryV2>
): NestedSelectOption[] => {
    return entityUrns.map((urn: string) => {
        const entity = entityCache.get(urn);
        if (entity) {
            return {
                value: entity.urn,
                label: entityRegistry.getDisplayName(entity.type, entity),
                id: entity.urn,
                parentId: (entity as any).parentDomains?.domains?.[0]?.urn,
                entity,
            };
        }
        
        // Fallback option when entity isn't hydrated yet
        // Extract name from URN (e.g., "urn:li:domain:engineering" -> "engineering")
        const entityName = urn.split(':').pop() || urn;
        return {
            value: urn,
            label: entityName,
            id: urn,
            parentId: undefined,
            entity: undefined,
        };
    });
};

/**
 * Create owner input objects from URNs
 */
export const createOwnerInputs = (selectedOwnerUrns: string[]) => {
    return selectedOwnerUrns.map((urn) => {
        // Determine owner type from URN (CorpGroup URNs typically contain 'corpGroup')
        const ownerEntityType = urn.includes('corpGroup') ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;

        return {
            ownerUrn: urn,
            ownerEntityType,
            type: OwnershipType.BusinessOwner,
        };
    });
};

/**
 * Merge selected entities into options to ensure they remain visible during search
 */
export const mergeSelectedIntoOptions = <T extends { value: string }>(
    options: T[], 
    selectedOptions: T[]
): T[] => {
    const optionUrns = new Set(options.map(option => option.value));
    const selectedNotInOptions = selectedOptions.filter(
        selected => !optionUrns.has(selected.value)
    );
    
    return [...options, ...selectedNotInOptions];
};

/**
 * Specialized version for NestedSelectOption merging
 */
export const mergeSelectedNestedOptions = (
    options: NestedSelectOption[], 
    selectedOptions: NestedSelectOption[]
): NestedSelectOption[] => {
    const optionUrns = new Set(options.map(option => option.value));
    const selectedNotInOptions = selectedOptions.filter(
        selected => !optionUrns.has(selected.value)
    );
    
    return [...options, ...selectedNotInOptions];
};
