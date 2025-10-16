import { useState, useMemo, useCallback } from 'react';
import { Entity } from '../../glossary.types';
import { HierarchyNameResolver } from '../utils/hierarchyUtils';

/**
 * Entity with children for hierarchical display
 */
export type HierarchicalEntity = Entity & { 
    children: HierarchicalEntity[];
    _indentLevel?: number;
    _indentSize?: number;
};

/**
 * Configuration for hierarchical data hook
 */
export interface UseHierarchicalDataOptions {
    /** Array of flat entities to organize hierarchically */
    entities: Entity[];
    /** Initial expanded row keys */
    initialExpandedKeys?: string[];
    /** Indent size in pixels per level */
    indentSize?: number;
}

/**
 * Hook for managing hierarchical entity data with expand/collapse functionality
 * 
 * Organizes flat entities into a tree structure, manages expansion state,
 * and provides flattened data for table rendering.
 * 
 * @example
 * ```tsx
 * const {
 *     hierarchicalData,
 *     flattenedData,
 *     expandedKeys,
 *     expandAll,
 *     collapseAll,
 *     toggleExpand
 * } = useHierarchicalData({ entities });
 * ```
 */
export function useHierarchicalData({
    entities,
    initialExpandedKeys = [],
    indentSize = 20,
}: UseHierarchicalDataOptions) {
    const [expandedKeys, setExpandedKeys] = useState<string[]>(initialExpandedKeys);

    /**
     * Organize flat entities into hierarchical structure
     */
    const hierarchicalData = useMemo(() => {
        const entityMap = new Map<string, HierarchicalEntity>();
        const rootEntities: HierarchicalEntity[] = [];

        // Create map of all entities with empty children arrays
        entities.forEach(entity => {
            entityMap.set(entity.name, { ...entity, children: [] });
        });

        /**
         * Find parent entity by parsing hierarchical name
         */
        const findParentEntity = (parentPath: string): HierarchicalEntity | null => {
            const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentPath);
            return entityMap.get(actualParentName) || null;
        };

        // Build hierarchy by linking children to parents
        entities.forEach(entity => {
            const parentNames = entity.data.parent_nodes
                ?.split(',')
                .map(name => name.trim())
                .filter(Boolean) || [];
            
            const entityNode = entityMap.get(entity.name)!;
            
            if (parentNames.length === 0) {
                // Root level entity
                rootEntities.push(entityNode);
            } else {
                // Find and attach to parent
                const parentName = parentNames[0];
                const parentEntity = findParentEntity(parentName);
                if (parentEntity) {
                    parentEntity.children.push(entityNode);
                } else {
                    // Parent not found, treat as root
                    rootEntities.push(entityNode);
                }
            }
        });

        return rootEntities;
    }, [entities]);

    /**
     * Flatten hierarchical data for table rendering with indentation
     */
    const flattenedData = useMemo(() => {
        const flatten = (
            entities: HierarchicalEntity[], 
            level: number = 0
        ): (HierarchicalEntity & { _indentLevel: number; _indentSize: number })[] => {
            const result: (HierarchicalEntity & { _indentLevel: number; _indentSize: number })[] = [];
            
            entities.forEach(entity => {
                // Add entity with indentation metadata
                result.push({
                    ...entity,
                    _indentLevel: level,
                    _indentSize: level * indentSize,
                });
                
                // Add children if expanded
                if (entity.children.length > 0 && expandedKeys.includes(entity.name)) {
                    result.push(...flatten(entity.children, level + 1));
                }
            });
            
            return result;
        };

        return flatten(hierarchicalData);
    }, [hierarchicalData, expandedKeys, indentSize]);

    /**
     * Collect all expandable keys recursively
     */
    const collectExpandableKeys = useCallback((entities: HierarchicalEntity[]): string[] => {
        const keys: string[] = [];
        entities.forEach(entity => {
            if (entity.children && entity.children.length > 0) {
                keys.push(entity.name);
                keys.push(...collectExpandableKeys(entity.children));
            }
        });
        return keys;
    }, []);

    /**
     * Expand all nodes in the hierarchy
     */
    const expandAll = useCallback(() => {
        const allKeys = collectExpandableKeys(hierarchicalData);
        setExpandedKeys(allKeys);
    }, [hierarchicalData, collectExpandableKeys]);

    /**
     * Collapse all nodes in the hierarchy
     */
    const collapseAll = useCallback(() => {
        setExpandedKeys([]);
    }, []);

    /**
     * Toggle expansion for a specific node
     */
    const toggleExpand = useCallback((key: string) => {
        setExpandedKeys(prev => 
            prev.includes(key) 
                ? prev.filter(k => k !== key)
                : [...prev, key]
        );
    }, []);

    /**
     * Set expanded keys manually
     */
    const setExpanded = useCallback((keys: string[]) => {
        setExpandedKeys(keys);
    }, []);

    return {
        /** Hierarchical tree structure */
        hierarchicalData,
        /** Flattened data with indentation for table rendering */
        flattenedData,
        /** Currently expanded keys */
        expandedKeys,
        /** Expand all nodes */
        expandAll,
        /** Collapse all nodes */
        collapseAll,
        /** Toggle a specific node */
        toggleExpand,
        /** Manually set expanded keys */
        setExpanded,
    };
}

