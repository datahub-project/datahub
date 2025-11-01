import { useMemo } from 'react';
import { Entity } from '../../glossary.types';

/**
 * Configuration for entity search
 */
export interface UseEntitySearchOptions {
    /** Array of entities to search through */
    entities: Entity[];
    /** Search query string */
    query?: string;
    /** Fields to search in the entity.data object, plus 'name' for entity.name */
    searchFields: string[];
    /** Status filter: '0' = all, '1' = new, '2' = updated, '3' = conflict */
    statusFilter?: string;
}

/**
 * Hook for searching and filtering entities
 * 
 * Provides efficient search across multiple entity fields with optional status filtering.
 * Uses memoization to prevent unnecessary recalculations.
 * 
 * @example
 * ```tsx
 * const filteredEntities = useEntitySearch({
 *     entities,
 *     query: 'customer',
 *     searchFields: ['name', 'description', 'term_source'],
 *     statusFilter: '1' // new entities only
 * });
 * ```
 */
export function useEntitySearch({
    entities,
    query,
    searchFields,
    statusFilter = '0',
}: UseEntitySearchOptions): Entity[] {
    return useMemo(() => {
        let filtered = entities;

        // Text search across specified fields
        if (query && query.trim()) {
            const searchLower = query.toLowerCase().trim();
            filtered = filtered.filter(entity => {
                // Check if any of the specified fields match the query
                return searchFields.some(field => {
                    // Handle entity.name separately from entity.data.field
                    const value = field === 'name' 
                        ? entity.name 
                        : entity.data[field];
                    
                    // Safely convert to string and search
                    return value?.toString().toLowerCase().includes(searchLower);
                });
            });
        }

        // Status filter
        if (statusFilter !== '0') {
            const statusMap = ['', 'new', 'updated', 'conflict'] as const;
            const targetStatus = statusMap[parseInt(statusFilter, 10)];
            if (targetStatus) {
                filtered = filtered.filter(entity => entity.status === targetStatus);
            }
        }

        return filtered;
    }, [entities, query, searchFields, statusFilter]);
}

/**
 * Predefined searchable fields for glossary entities
 */
export const GLOSSARY_SEARCHABLE_FIELDS: string[] = [
    'name',
    'description',
    'term_source',
    'source_ref',
    'source_url',
    'ownership_users',
    'ownership_groups',
    'related_contains',
    'related_inherits',
    'domain_name',
    'custom_properties',
];

