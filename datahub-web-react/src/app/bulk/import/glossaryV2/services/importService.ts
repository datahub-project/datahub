/**
 * Import orchestration and batch processing service
 */

import { Entity, HierarchyMaps, ImportProgress, ImportResult } from '../types';
import { BATCH_SIZE } from '../utils/constants';

export class ImportService {
  /**
   * Process unified entities in batches with two-phase approach
   */
  static async processUnifiedBatchOfEntities(
    entityBatch: Entity[], 
    batchIndex: number,
    patchEntitiesMutation: any,
    updateProgress: (updates: Partial<ImportProgress>) => void,
    createdEntities: Map<string, Entity>,
    setHierarchyMaps: (updater: (prev: HierarchyMaps | null) => HierarchyMaps | null) => void,
    setAllEntities: (updater: (prev: Entity[]) => Entity[]) => void
  ): Promise<void> {
    
    // Filter entities that have changes (new or updated status)
    const entitiesWithChanges = entityBatch.filter(entity => 
      entity.status === 'new' || entity.status === 'updated'
    );
    
    // If no entities have changes, skip this batch
    if (entitiesWithChanges.length === 0) {
      updateProgress({ currentOperation: `Skipping batch ${batchIndex + 1} - no changes` });
      return;
    }
    
    // PHASE 1: Create/update entities with basic properties and parent relationships
    const basicPatches: any[] = [];
    const batchItemMap: { [patchIndex: number]: { originalIndex: number; entity: Entity } } = {};
    
    entitiesWithChanges.forEach((entity, batchRowIndex) => {
      const originalIndex = entityBatch.indexOf(entity);
      const entityPatches = this.buildEntityPatchesFromUnifiedEntity(entity);
      
      entityPatches.forEach((patch, patchOffset) => {
        basicPatches.push(patch);
        batchItemMap[basicPatches.length - 1] = { originalIndex, entity };
      });
    });
    
    // Execute basic patch mutation
    try {
      updateProgress({ currentOperation: `Phase 1: Creating/updating entities in batch ${batchIndex + 1} (${entitiesWithChanges.length} entities)` });
      
      const result = await patchEntitiesMutation({
        variables: { input: basicPatches }
      });
      
      if (!result.data?.patchEntities) {
        throw new Error('No patch results returned');
      }
      
      // Process results and collect URNs for phase 2
      const entitiesWithUrns: Entity[] = [];
      result.data.patchEntities.forEach((patchResult: any, patchIndex: number) => {
        const { originalIndex, entity } = batchItemMap[patchIndex];
        
        if (patchResult.success) {
          // Update hierarchy maps and allEntities with new URN for future parent resolution
          if (patchResult.urn) {
            const updatedEntity = { ...entity, urn: patchResult.urn };
            
            // Update local tracking map immediately for next level processing
            createdEntities.set(entity.name, updatedEntity);
            
            // Update hierarchy maps
            setHierarchyMaps(prev => {
              if (!prev) return prev;
              
              // Update entity in maps
              prev.entitiesByName.set(entity.name, updatedEntity);
              prev.entitiesById.set(entity.id, updatedEntity);
              
              // Update the entity in the level-specific map
              const levelEntities = prev.entitiesByLevel.get(entity.level);
              if (levelEntities) {
                const entityIndex = levelEntities.findIndex(e => e.id === entity.id);
                if (entityIndex >= 0) {
                  levelEntities[entityIndex] = updatedEntity;
                }
              }
              
              return prev;
            });
            
            // Update allEntities state so future levels can find this entity
            setAllEntities(prev => {
              const updatedEntities = prev.map(e => 
                e.id === entity.id ? updatedEntity : e
              );
              return updatedEntities;
            });
            
            // Collect entity with URN for phase 2
            entitiesWithUrns.push(updatedEntity);
          }
        } else {
          console.error(`❌ Failed to process ${entity.type} "${entity.name}": ${patchResult.error}`);
          throw new Error(`Failed to process ${entity.type} "${entity.name}": ${patchResult.error}`);
        }
      });
      
      // PHASE 2: Apply domain and ownership patches using the URNs from phase 1
      if (entitiesWithUrns.length > 0) {
        updateProgress({ currentOperation: `Phase 2: Applying domain and ownership patches for batch ${batchIndex + 1}` });
        
        const domainAndOwnershipPatches: any[] = [];
        const phase2ItemMap: { [patchIndex: number]: { originalIndex: number; entity: Entity } } = {};
        
        entitiesWithUrns.forEach((entity, entityIndex) => {
          const originalIndex = entityBatch.indexOf(entity);
          
          // Add domain patches
          const domainPatches = this.buildDomainPatches(entity);
          domainPatches.forEach((patch, patchOffset) => {
            domainAndOwnershipPatches.push(patch);
            phase2ItemMap[domainAndOwnershipPatches.length - 1] = { originalIndex, entity };
          });
          
          // Add ownership patches
          const ownershipPatches = this.buildOwnershipPatches(entity);
          ownershipPatches.forEach((patch, patchOffset) => {
            domainAndOwnershipPatches.push(patch);
            phase2ItemMap[domainAndOwnershipPatches.length - 1] = { originalIndex, entity };
          });
        });
        
        // Execute domain and ownership patches
        if (domainAndOwnershipPatches.length > 0) {
          const phase2Result = await patchEntitiesMutation({
            variables: { input: domainAndOwnershipPatches }
          });
          
          if (!phase2Result.data?.patchEntities) {
            throw new Error('No phase 2 patch results returned');
          }
          
          // Process phase 2 results
          phase2Result.data.patchEntities.forEach((patchResult: any, patchIndex: number) => {
            const { originalIndex, entity } = phase2ItemMap[patchIndex];
            
            if (!patchResult.success) {
              console.warn(`⚠️ Phase 2 patch failed for ${entity.type} "${entity.name}": ${patchResult.error}`);
            }
          });
        }
      }
      
    } catch (error) {
      console.error(`❌ Unified batch ${batchIndex + 1} failed:`, error);
      throw error; // Re-throw for fail-fast behavior
    }
  }

  /**
   * Create processing batches
   */
  static createProcessingBatches(entities: Entity[], batchSize: number = BATCH_SIZE): Entity[][] {
    const batches: Entity[][] = [];
    for (let i = 0; i < entities.length; i += batchSize) {
      batches.push(entities.slice(i, i + batchSize));
    }
    return batches;
  }

  /**
   * Validate import prerequisites
   */
  static validateImportPrerequisites(entities: Entity[]): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];

    // Check for required fields
    entities.forEach((entity, index) => {
      if (!entity.name || entity.name.trim() === '') {
        errors.push(`Entity ${index + 1}: Name is required`);
      }
      if (!entity.type || !['glossaryTerm', 'glossaryNode'].includes(entity.type)) {
        errors.push(`Entity ${index + 1}: Invalid entity type`);
      }
    });

    // Check for circular dependencies
    const circularDeps = this.detectCircularDependencies(entities);
    if (circularDeps.length > 0) {
      errors.push(`Circular dependencies detected: ${circularDeps.join(', ')}`);
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  /**
   * Detect circular dependencies
   */
  private static detectCircularDependencies(entities: Entity[]): string[] {
    const circularDependencies: string[] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    const hasCycle = (entityName: string, path: string[]): boolean => {
      if (recursionStack.has(entityName)) {
        circularDependencies.push([...path, entityName].join(' -> '));
        return true;
      }

      if (visited.has(entityName)) {
        return false;
      }

      visited.add(entityName);
      recursionStack.add(entityName);

      const entity = entities.find(e => e.name === entityName);
      if (entity) {
        for (const parentName of entity.parentNames) {
          if (hasCycle(parentName, [...path, entityName])) {
            return true;
          }
        }
      }

      recursionStack.delete(entityName);
      return false;
    };

    entities.forEach(entity => {
      if (!visited.has(entity.name)) {
        hasCycle(entity.name, []);
      }
    });

    return circularDependencies;
  }

  /**
   * Build entity patches from unified entity (only basic properties and parent relationships)
   */
  private static buildEntityPatchesFromUnifiedEntity(entity: Entity): any[] {
    const patches: any[] = [];
    
    // Primary aspect based on entity type (only basic properties and parent-child relationships)
    if (entity.type === 'glossaryTerm') {
      patches.push(this.buildGlossaryTermInfoPatch(entity.data, entity.urn, entity.parentUrns));
    } else if (entity.type === 'glossaryNode') {
      patches.push(this.buildGlossaryNodeInfoPatch(entity.data, entity.urn, entity.parentUrns));
    } else {
      throw new Error(`Unsupported entity type: ${entity.type}`);
    }
    
    return patches;
  }

  /**
   * Build domain patches for GlossaryTerm entities (to be run after entity creation)
   */
  private static buildDomainPatches(entity: Entity): any[] {
    const patches: any[] = [];
    
    if (entity.type === 'glossaryTerm' && entity.data.domain_urn) {
      const domainPatch = this.buildDomainPatch(entity.data, entity.urn);
      if (domainPatch) {
        patches.push(domainPatch);
      }
    }
    
    return patches;
  }

  /**
   * Build ownership patches for both entity types (to be run after entity creation)
   */
  private static buildOwnershipPatches(entity: Entity): any[] {
    const patches: any[] = [];
    
    if (entity.data.ownership) {
      patches.push(this.buildOwnershipPatch(entity.data, entity.type, entity.urn));
    }
    
    return patches;
  }

  /**
   * Build glossaryTermInfo patch operations
   */
  private static buildGlossaryTermInfoPatch(row: any, urn?: string, parentUrns?: string[]): any {
    const patches: any[] = [];
    
    // For new entities (no URN), mimic existing mutation behavior - convert null to ""
    if (!urn) {
      // Required fields for new entities (mimic createGlossaryTerm)
      patches.push({ op: 'ADD', path: '/name', value: row.name || null });
      patches.push({ op: 'ADD', path: '/definition', value: (row.definition || row.description) || "" });
      patches.push({ op: 'ADD', path: '/termSource', value: row.term_source || 'INTERNAL' });
    } else {
      // For existing entities, only patch fields that have actual values
      if (row.name) patches.push({ op: 'ADD', path: '/name', value: row.name });
      if (row.definition || row.description) {
        patches.push({ op: 'ADD', path: '/definition', value: row.definition || row.description });
      }
      if (row.term_source) patches.push({ op: 'ADD', path: '/termSource', value: row.term_source });
    }
    
    // Optional fields
    if (row.source_ref) {
      patches.push({ op: 'ADD', path: '/sourceRef', value: row.source_ref });
    }
    if (row.source_url) {
      patches.push({ op: 'ADD', path: '/sourceUrl', value: row.source_url });
    }
    
    // Custom properties - create individual patch operations for each property
    if (row.custom_properties) {
      try {
        const customProps = typeof row.custom_properties === 'string' 
          ? JSON.parse(row.custom_properties)
          : row.custom_properties;
        
        Object.entries(customProps).forEach(([key, value]) => {
          patches.push({ 
            op: 'ADD', 
            path: `/customProperties/${key}`,
            value: JSON.stringify(String(value))
          });
        });
      } catch (e) {
        console.warn(`Failed to parse custom properties for ${row.name}:`, e);
      }
    }
    
    // Add parent-child relationships
    if (parentUrns && parentUrns.length > 0) {
      const immediateParentUrn = parentUrns[parentUrns.length - 1];
      patches.push({
        op: 'ADD',
        path: '/parentNode',
        value: immediateParentUrn
      });
    }
    
    return {
      urn,
      entityType: 'glossaryTerm',
      aspectName: 'glossaryTermInfo',
      patch: patches
    };
  }

  /**
   * Build glossaryNodeInfo patch operations
   */
  private static buildGlossaryNodeInfoPatch(row: any, urn?: string, parentUrns?: string[]): any {
    const patches: any[] = [];
    
    // For new entities (no URN), mimic existing mutation behavior
    if (!urn) {
      patches.push({ op: 'ADD', path: '/name', value: row.name || null });
      patches.push({ op: 'ADD', path: '/definition', value: (row.description || row.definition) || "" });
    } else {
      if (row.name) patches.push({ op: 'ADD', path: '/name', value: row.name });
      if (row.description || row.definition) {
        patches.push({ op: 'ADD', path: '/definition', value: row.description || row.definition });
      }
    }
    
    // Custom properties
    if (row.custom_properties) {
      try {
        const customProps = typeof row.custom_properties === 'string' 
          ? JSON.parse(row.custom_properties)
          : row.custom_properties;
        
        Object.entries(customProps).forEach(([key, value]) => {
          patches.push({ 
            op: 'ADD', 
            path: `/customProperties/${key}`,
            value: JSON.stringify(String(value))
          });
        });
      } catch (e) {
        console.warn(`Failed to parse custom properties for ${row.name}:`, e);
      }
    }
    
    // Add parent-child relationships
    if (parentUrns && parentUrns.length > 0) {
      const immediateParentUrn = parentUrns[parentUrns.length - 1];
      patches.push({
        op: 'ADD',
        path: '/parentNode',
        value: immediateParentUrn
      });
    }
    
    return {
      urn,
      entityType: 'glossaryNode', 
      aspectName: 'glossaryNodeInfo',
      patch: patches
    };
  }

  /**
   * Build domain patch operations for GlossaryTerm entities
   * Note: GlossaryTerm entities don't have a domains aspect, so we return null
   * Domain assignment should be handled via setDomain mutation instead
   */
  private static buildDomainPatch(row: any, urn?: string): any {
    // GlossaryTerm entities don't support domain patches via the domains aspect
    // Domain assignment should be handled via setDomain mutation
    return null;
  }

  /**
   * Build ownership patch operations for both entity types
   */
  private static buildOwnershipPatch(row: any, entityType: string, urn?: string): any {
    const patches: any[] = [];
    
    if (row.ownership) {
      try {
        const ownershipEntries = this.parseOwnershipString(row.ownership);
        ownershipEntries.forEach((entry) => {
          patches.push({
            op: 'ADD',
            path: `/owners/${entry.owner}/${entry.type}`,
            value: JSON.stringify({
              owner: entry.owner,
              type: entry.type
            })
          });
        });
      } catch (e) {
        console.warn(`Failed to parse ownership for ${row.name}:`, e);
      }
    }
    
    return {
      urn,
      entityType,
      aspectName: 'ownership',
      patch: patches
    };
  }

  /**
   * Parse ownership string into structured format
   */
  private static parseOwnershipString(ownershipStr: string): Array<{owner: string, type: string, ownerType: string}> {
    if (!ownershipStr || !ownershipStr.trim()) return [];
    
    return ownershipStr.split(',').map(item => {
      const trimmed = item.trim();
      if (trimmed.includes(':')) {
        const parts = trimmed.split(':').map(s => s.trim());
        if (parts.length >= 3) {
          // Format: owner:type:ownerType
          const [owner, type, ownerType] = parts;
          return { 
            owner: this.formatUserUrn(owner), 
            type, 
            ownerType 
          };
        } else if (parts.length === 2) {
          // Format: owner:type (assume CORP_USER)
          const [owner, type] = parts;
          return { 
            owner: this.formatUserUrn(owner), 
            type, 
            ownerType: 'CORP_USER' 
          };
        }
      }
      // Fallback: treat as owner with default type
      return { 
        owner: this.formatUserUrn(trimmed), 
        type: 'DATAOWNER', 
        ownerType: 'CORP_USER' 
      };
    });
  }

  /**
   * Format user identifier as proper URN
   */
  private static formatUserUrn(userInput: string): string {
    if (!userInput) return userInput;
    
    // If already a URN, return as-is
    if (userInput.startsWith('urn:li:corpuser:')) {
      return userInput;
    }
    
    // If it's just a username, add the URN prefix
    return `urn:li:corpuser:${userInput}`;
  }
}
