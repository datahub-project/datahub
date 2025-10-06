import { useState, useCallback, useRef } from 'react';
import { ApolloClient } from '@apollo/client';
import { Entity, EntityData, HierarchyMaps, ValidationError, ValidationWarning, EntityPatchInput, PatchOperation } from '../../glossary.types';
import { useGraphQLOperations } from './useGraphQLOperations';
import { useHierarchyManagement } from './useHierarchyManagement';
import { useEntityManagement } from './useEntityManagement';
import { useEntityComparison } from './useEntityComparison';

export interface ImportProgress {
  total: number;
  processed: number;
  successful: number;
  failed: number;
  currentEntity?: Entity;
  currentOperation?: string;
  errors: ImportError[];
  warnings: ImportWarning[];
}

export interface ImportError {
  entityId: string;
  entityName: string;
  operation: string;
  error: string;
  retryable: boolean;
}

export interface ImportWarning {
  entityId: string;
  entityName: string;
  operation: string;
  message: string;
}

export interface ImportBatch {
  entities: Entity[];
  level: number;
  dependencies: string[];
  existingEntities: Entity[];
}

export interface UseImportProcessingProps {
  apolloClient: ApolloClient<any>;
  onProgress?: (progress: ImportProgress) => void;
  batchSize?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export interface UseImportProcessingReturn {
  progress: ImportProgress;
  isProcessing: boolean;
  startImport: (entities: Entity[], existingEntities: Entity[]) => Promise<void>;
  pauseImport: () => void;
  resumeImport: () => void;
  cancelImport: () => void;
  retryFailed: () => Promise<void>;
  resetProgress: () => void;
  createProcessingBatches: (entities: Entity[], hierarchyMaps: HierarchyMaps) => ImportBatch[];
  processBatch: (batch: ImportBatch) => Promise<void>;
}

export const useImportProcessing = ({
  apolloClient,
  onProgress,
  batchSize = 50,
  maxRetries = 3,
  retryDelay = 1000,
}: UseImportProcessingProps): UseImportProcessingReturn => {
  const [progress, setProgress] = useState<ImportProgress>({
    total: 0,
    processed: 0,
    successful: 0,
    failed: 0,
    errors: [],
    warnings: [],
  });

  const [isProcessing, setIsProcessing] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [isCancelled, setIsCancelled] = useState(false);

  const processingQueueRef = useRef<ImportBatch[]>([]);
  const currentBatchRef = useRef<ImportBatch | null>(null);
  const retryCountRef = useRef<Map<string, number>>(new Map());
  
  // Track generated URNs for relationship resolution
  const entityUrnMap = useRef<Map<string, string>>(new Map()); // entity name -> URN
  
  // Track ownership type name -> URN mapping
  const ownershipTypeMapRef = useRef<Map<string, string>>(new Map()); // type name -> URN

  const { 
    executeUnifiedGlossaryQuery, 
    executePatchEntitiesMutation,
    executeAddRelatedTermsMutation,
    executeSetDomainMutation,
    executeBatchSetDomainMutation,
    executeGetOwnershipTypesQuery
  } = useGraphQLOperations();
  const { createProcessingOrder, validateHierarchy } = useHierarchyManagement();
  const { normalizeCsvData, compareEntities } = useEntityManagement();
  const { categorizeEntities, detectConflicts, getChangeDetails } = useEntityComparison();

  const updateProgress = useCallback((updates: Partial<ImportProgress>) => {
    setProgress(prev => {
      const newProgress = { ...prev, ...updates };
      onProgress?.(newProgress);
      return newProgress;
    });
  }, [onProgress]);

  const addError = useCallback((error: ImportError) => {
    setProgress(prev => ({
      ...prev,
      errors: [...prev.errors, error],
      failed: prev.failed + 1,
    }));
  }, []);

  const addWarning = useCallback((warning: ImportWarning) => {
    setProgress(prev => ({
      ...prev,
      warnings: [...prev.warnings, warning],
    }));
  }, []);

  /**
   * Handle all related terms (HasA and IsA relationships) in a single batched patch
   */
  const handleAllRelatedTerms = useCallback(async (entity: Entity, entityUrn: string, existingEntities: Entity[] = []) => {
    try {
      // Process both HasA and IsA relationships
      const hasARelatedNames = entity.data.related_contains?.split(',').map(name => name.trim()) || [];
      const isARelatedNames = entity.data.related_inherits?.split(',').map(name => name.trim()) || [];
      
      if (hasARelatedNames.length === 0 && isARelatedNames.length === 0) return;

      const hasARelatedUrns: string[] = [];
      const isARelatedUrns: string[] = [];
      
      // Helper function to resolve entity names to URNs
      const resolveEntityName = (relatedName: string): string | null => {
        if (!relatedName) return null;
        
        // Look up related entity URN from our map (newly created) or existing entities
        let relatedUrn = entityUrnMap.current.get(relatedName);
        
        // If not found with exact name, try hierarchical name matching for newly created entities
        if (!relatedUrn) {
          // Try to find by simple name if CSV has hierarchical name
          const csvSimpleName = relatedName.split('.').pop() || relatedName;
          if (csvSimpleName !== relatedName) {
            relatedUrn = entityUrnMap.current.get(csvSimpleName);
          }
          
          // Try to find by hierarchical name if CSV has simple name
          if (!relatedUrn) {
            for (const [entityName, urn] of entityUrnMap.current.entries()) {
              const simpleName = entityName.split('.').pop() || entityName;
              if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
                relatedUrn = urn;
                break;
              }
            }
          }
        }
        
        // If not found in newly created entities, look in existing entities
        if (!relatedUrn) {
          const existingEntity = existingEntities.find(e => {
            // Try exact match first
            if (e.name.toLowerCase() === relatedName.toLowerCase()) {
              return true;
            }
            
            // Try hierarchical name match (extract simple name from hierarchical)
            const simpleName = e.name.split('.').pop() || e.name;
            if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
              return true;
            }
            
            // Try reverse - if CSV has hierarchical name, match against simple name
            const csvSimpleName = relatedName.split('.').pop() || relatedName;
            if (e.name.toLowerCase() === csvSimpleName.toLowerCase()) {
              return true;
            }
            
            return false;
          });
          
          if (existingEntity && existingEntity.urn) {
            relatedUrn = existingEntity.urn;
          }
        }
        
        if (!relatedUrn) {
          addWarning({
            entityId: entity.id,
            entityName: entity.name,
            operation: 'relationship',
            message: `Related entity "${relatedName}" not found`
          });
        }
        
        return relatedUrn || null;
      };

      // Resolve HasA relationships
      for (const relatedName of hasARelatedNames) {
        const relatedUrn = resolveEntityName(relatedName);
        if (relatedUrn) {
          hasARelatedUrns.push(relatedUrn);
        }
      }

      // Resolve IsA relationships
      for (const relatedName of isARelatedNames) {
        const relatedUrn = resolveEntityName(relatedName);
        if (relatedUrn) {
          isARelatedUrns.push(relatedUrn);
        }
      }

      // Create a single batched patch for all relationships
      if (hasARelatedUrns.length > 0 || isARelatedUrns.length > 0) {
        const relationshipPatch: EntityPatchInput = {
          entityType: 'glossaryTerm',
          urn: entityUrn,
          aspectName: 'glossaryRelatedTerms',
          patch: [
            {
              op: 'ADD',
              path: '/hasRelatedTerms',
              value: JSON.stringify(hasARelatedUrns)
            },
            {
              op: 'ADD',
              path: '/isRelatedTerms',
              value: JSON.stringify(isARelatedUrns)
            }
          ]
        };
        
        // Execute the single relationship patch
        await executePatchEntitiesMutation([relationshipPatch]);
      }
    } catch (error) {
      console.error(`Failed to set relationships for ${entity.name}:`, error);
      throw error;
    }
  }, [executePatchEntitiesMutation, addWarning]);

  /**
   * Handle batch relationships for multiple entities in a single patchEntities call
   */
  const handleBatchRelationships = useCallback(async (entities: Entity[], existingEntities: Entity[] = []) => {
    try {
      const relationshipPatches: EntityPatchInput[] = [];
      
      for (const entity of entities) {
        const entityUrn = entityUrnMap.current.get(entity.name);
        if (!entityUrn) continue;

        // Process both HasA and IsA relationships
        const hasARelatedNames = entity.data.related_contains?.split(',').map(name => name.trim()) || [];
        const isARelatedNames = entity.data.related_inherits?.split(',').map(name => name.trim()) || [];
        
        if (hasARelatedNames.length === 0 && isARelatedNames.length === 0) continue;

        const hasARelatedUrns: string[] = [];
        const isARelatedUrns: string[] = [];
        
        // Helper function to resolve entity names to URNs
        const resolveEntityName = (relatedName: string): string | null => {
          if (!relatedName) return null;
          
          // Look up related entity URN from our map (newly created) or existing entities
          let relatedUrn = entityUrnMap.current.get(relatedName);
          
          // If not found with exact name, try hierarchical name matching for newly created entities
          if (!relatedUrn) {
            // Try to find by simple name if CSV has hierarchical name
            const csvSimpleName = relatedName.split('.').pop() || relatedName;
            if (csvSimpleName !== relatedName) {
              relatedUrn = entityUrnMap.current.get(csvSimpleName);
            }
            
            // Try to find by hierarchical name if CSV has simple name
            if (!relatedUrn) {
              for (const [entityName, urn] of entityUrnMap.current.entries()) {
                const simpleName = entityName.split('.').pop() || entityName;
                if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
                  relatedUrn = urn;
                  break;
                }
              }
            }
          }
          
          // If not found in newly created entities, look in existing entities
          if (!relatedUrn) {
            const existingEntity = existingEntities.find(e => {
              // Try exact match first
              if (e.name.toLowerCase() === relatedName.toLowerCase()) {
                return true;
              }
              
              // Try hierarchical name match (extract simple name from hierarchical)
              const simpleName = e.name.split('.').pop() || e.name;
              if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
                return true;
              }
              
              // Try reverse - if CSV has hierarchical name, match against simple name
              const csvSimpleName = relatedName.split('.').pop() || relatedName;
              if (e.name.toLowerCase() === csvSimpleName.toLowerCase()) {
                return true;
              }
              
              return false;
            });
            
            if (existingEntity && existingEntity.urn) {
              relatedUrn = existingEntity.urn;
            }
          }
          
          if (!relatedUrn) {
            addWarning({
              entityId: entity.id,
              entityName: entity.name,
              operation: 'relationship',
              message: `Related entity "${relatedName}" not found`
            });
          }
          
          return relatedUrn || null;
        };

        // Resolve HasA relationships
        for (const relatedName of hasARelatedNames) {
          const relatedUrn = resolveEntityName(relatedName);
          if (relatedUrn) {
            hasARelatedUrns.push(relatedUrn);
          }
        }

        // Resolve IsA relationships
        for (const relatedName of isARelatedNames) {
          const relatedUrn = resolveEntityName(relatedName);
          if (relatedUrn) {
            isARelatedUrns.push(relatedUrn);
          }
        }

        // Create relationship patch for this entity
        if (hasARelatedUrns.length > 0 || isARelatedUrns.length > 0) {
          relationshipPatches.push({
            entityType: 'glossaryTerm',
            urn: entityUrn,
            aspectName: 'glossaryRelatedTerms',
            patch: [
              {
                op: 'ADD',
                path: '/hasRelatedTerms',
                value: JSON.stringify(hasARelatedUrns)
              },
              {
                op: 'ADD',
                path: '/isRelatedTerms',
                value: JSON.stringify(isARelatedUrns)
              }
            ]
          });
        }
      }

      // Execute all relationship patches in a single batch
      if (relationshipPatches.length > 0) {
        updateProgress({
          currentOperation: `Setting relationships for ${relationshipPatches.length} entities...`,
        });
        
        await executePatchEntitiesMutation(relationshipPatches);
      }
    } catch (error) {
      addWarning({
        entityId: 'batch',
        entityName: 'Batch',
        operation: 'relationships',
        message: `Failed to set relationships: ${error instanceof Error ? error.message : 'Unknown error'}`
      });
    }
  }, [executePatchEntitiesMutation, addWarning, updateProgress]);

  /**
   * Handle entity relationships after creation
   */
  const handleEntityRelationships = useCallback(async (entity: Entity, entityUrn: string, existingEntities: Entity[] = []) => {
    if (!entityUrn) return;

    try {
      // Parent relationships are handled in patchEntities mutation

      // Handle all relationships in a single batched patch
      await handleAllRelatedTerms(entity, entityUrn, existingEntities);

      // Handle domain assignment
      if (entity.data.domain_urn) {
        await executeSetDomainMutation(entityUrn, entity.data.domain_urn);
      }

    } catch (error) {
      addWarning({
        entityId: entity.id,
        entityName: entity.name,
        operation: 'relationship',
        message: `Failed to create relationships: ${error instanceof Error ? error.message : 'Unknown error'}`
      });
    }
  }, [executePatchEntitiesMutation, executeSetDomainMutation, addWarning, handleAllRelatedTerms]);

  /**
   * Handle related terms (HasA or IsA relationships) using patchEntities
   */
  const handleRelatedTerms = useCallback(async (entity: Entity, entityUrn: string, relationshipType: 'HasA' | 'IsA', existingEntities: Entity[] = []) => {
    try {
      const relatedNames = relationshipType === 'HasA' 
        ? entity.data.related_contains?.split(',').map(name => name.trim())
        : entity.data.related_inherits?.split(',').map(name => name.trim());
      
      if (!relatedNames || relatedNames.length === 0) return;

      const relatedUrns: string[] = [];
      
      for (const relatedName of relatedNames) {
        if (!relatedName) continue;
        
        // Look up related entity URN from our map (newly created) or existing entities
        let relatedUrn = entityUrnMap.current.get(relatedName);
        
        // If not found with exact name, try hierarchical name matching for newly created entities
        if (!relatedUrn) {
          // Try to find by simple name if CSV has hierarchical name
          const csvSimpleName = relatedName.split('.').pop() || relatedName;
          if (csvSimpleName !== relatedName) {
            relatedUrn = entityUrnMap.current.get(csvSimpleName);
          }
          
          // Try to find by hierarchical name if CSV has simple name
          if (!relatedUrn) {
            for (const [entityName, urn] of entityUrnMap.current.entries()) {
              const simpleName = entityName.split('.').pop() || entityName;
              if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
                relatedUrn = urn;
                break;
              }
            }
          }
        }
        
        // If not found in newly created entities, look in existing entities
        if (!relatedUrn) {
          const existingEntity = existingEntities.find(e => {
            // Try exact match first
            if (e.name.toLowerCase() === relatedName.toLowerCase()) {
              return true;
            }
            
            // Try hierarchical name match (extract simple name from hierarchical)
            const simpleName = e.name.split('.').pop() || e.name;
            if (simpleName.toLowerCase() === relatedName.toLowerCase()) {
              return true;
            }
            
            // Try reverse - if CSV has hierarchical name, match against simple name
            const csvSimpleName = relatedName.split('.').pop() || relatedName;
            if (e.name.toLowerCase() === csvSimpleName.toLowerCase()) {
              return true;
            }
            
            return false;
          });
          
          if (existingEntity && existingEntity.urn) {
            relatedUrn = existingEntity.urn;
          }
        }
        
        if (relatedUrn) {
          relatedUrns.push(relatedUrn);
        } else {
          addWarning({
            entityId: entity.id,
            entityName: entity.name,
            operation: 'relationship',
            message: `Related entity "${relatedName}" not found for ${relationshipType} relationship`
          });
        }
      }

      if (relatedUrns.length > 0) {
        // Create the glossaryRelatedTerms aspect with relationships using JSON stringified values
        const pathField = relationshipType === 'HasA' ? 'hasRelatedTerms' : 'isRelatedTerms';
        
        const relationshipPatch: EntityPatchInput = {
          entityType: 'glossaryTerm',
          urn: entityUrn,
          aspectName: 'glossaryRelatedTerms',
          patch: [
            {
              op: 'ADD',
              path: `/${pathField}`,
              value: JSON.stringify(relatedUrns)
            },
            {
              op: 'ADD',
              path: `/${pathField === 'hasRelatedTerms' ? 'isRelatedTerms' : 'hasRelatedTerms'}`,
              value: '[]'
            }
          ]
        };
        
        // Execute the relationship patch
        await executePatchEntitiesMutation([relationshipPatch]);
      }
    } catch (error) {
      console.error(`Failed to set ${relationshipType} relationships for ${entity.name}:`, error);
      throw error;
    }
  }, [executePatchEntitiesMutation, addWarning]);

  const createProcessingBatches = useCallback((entities: Entity[], hierarchyMaps: HierarchyMaps, existingEntities: Entity[] = []): ImportBatch[] => {
    const processingOrder = createProcessingOrder(entities);
    const batches: ImportBatch[] = [];
    
    // Group entities by hierarchy level
    const entitiesByLevel = new Map<number, Entity[]>();
    processingOrder.forEach(entity => {
      const level = entity.level;
      if (!entitiesByLevel.has(level)) {
        entitiesByLevel.set(level, []);
      }
      entitiesByLevel.get(level)!.push(entity);
    });

    // Create batches for each level
    entitiesByLevel.forEach((levelEntities, level) => {
      for (let i = 0; i < levelEntities.length; i += batchSize) {
        const batchEntities = levelEntities.slice(i, i + batchSize);
        const dependencies = batchEntities.flatMap(entity => entity.parentNames);
        
        batches.push({
          entities: batchEntities,
          level,
          dependencies,
          existingEntities,
        });
      }
    });

    return batches;
  }, [batchSize, createProcessingOrder]);


  /**
   * Process a batch of entities using a single patchEntities mutation
   */
  /**
   * Create ownership type patches for missing types
   */
  const createOwnershipTypePatches = useCallback((missingTypes: string[]): EntityPatchInput[] => {
    return missingTypes.map(typeName => ({
      entityType: "ownershipType",
      aspectName: "ownershipTypeInfo", 
      patch: [
        { op: "ADD", path: "/name", value: typeName },
        { op: "ADD", path: "/description", value: `Custom ownership type: ${typeName}` }
      ]
    }));
  }, []);

  /**
   * Ensure all required ownership types exist, creating missing ones
   */
  const ensureOwnershipTypesExist = useCallback(async (entities: Entity[]): Promise<void> => {
    try {
      // Extract all ownership types from entities
      const csvOwnershipTypes = new Set<string>();
      entities.forEach(entity => {
        if (entity.data.ownership) {
          const types = entity.data.ownership.split(',').map(owner => {
            const parts = owner.split(':');
            return parts[0]; // ownership type
          });
          types.forEach(type => csvOwnershipTypes.add(type));
        }
      });

      if (csvOwnershipTypes.size === 0) return;

      // Load existing ownership types
      updateProgress({
        currentOperation: `Loading existing ownership types...`,
      });

      const existingOwnershipTypes = await executeGetOwnershipTypesQuery({
        input: { start: 0, count: 1000 }
      });

      const ownershipTypeMap = new Map<string, string>();
      existingOwnershipTypes.forEach(type => {
        ownershipTypeMap.set(type.info.name.toLowerCase(), type.urn);
      });

      // Find missing ownership types
      const missingTypes = Array.from(csvOwnershipTypes).filter(
        type => !ownershipTypeMap.has(type.toLowerCase())
      );

      if (missingTypes.length > 0) {
        updateProgress({
          currentOperation: `Creating ${missingTypes.length} missing ownership types...`,
        });

        // Create missing ownership types using batch patch
        const ownershipTypePatches = createOwnershipTypePatches(missingTypes);
        const results = await executePatchEntitiesMutation(ownershipTypePatches);

        // Process results and update the map
        for (let i = 0; i < results.length; i++) {
          const result = results[i];
          const typeName = missingTypes[i];
          
          if (result.success && result.urn) {
            ownershipTypeMap.set(typeName.toLowerCase(), result.urn);
          } else {
            addError({
              entityId: 'ownership-type',
              entityName: typeName,
              operation: 'create-ownership-type',
              error: result.error || 'Failed to create ownership type',
              retryable: false
            });
          }
        }
      }

      // Store the ownership type map for use in ownership patches
      ownershipTypeMapRef.current = ownershipTypeMap;

    } catch (error) {
      addError({
        entityId: 'ownership-types',
        entityName: 'Batch',
        operation: 'load-ownership-types',
        error: `Failed to load/create ownership types: ${error instanceof Error ? error.message : 'Unknown error'}`,
        retryable: false
      });
    }
  }, [executeGetOwnershipTypesQuery, executePatchEntitiesMutation, createOwnershipTypePatches, updateProgress, addError]);

  /**
   * Create ownership patch for an entity
   */
  const createOwnershipPatch = useCallback((entity: Entity, urn: string): EntityPatchInput | null => {
    if (!entity.data.ownership) return null;
    
    try {
      const patches: PatchOperation[] = [];
      const ownershipStrings = entity.data.ownership.split(',').map(owner => owner.trim());
      
      ownershipStrings.forEach((ownerString) => {
        // Parse ownership string format: "type:owner" or "type:owner:corpType"
        const parts = ownerString.split(':');
        if (parts.length >= 2) {
          const [type, owner, corpType] = parts;
          
          // Format owner URN
          const ownerUrn = owner.startsWith('urn:li:') 
            ? owner 
            : `urn:li:corpuser:${owner}`;
          
          // Get ownership type URN from the map
          const ownershipTypeUrn = ownershipTypeMapRef.current.get(type.toLowerCase());
          if (!ownershipTypeUrn) {
            addError({
              entityId: entity.id,
              entityName: entity.name,
              operation: 'ownership',
              error: `Ownership type "${type}" not found. Please ensure it exists in DataHub.`,
              retryable: false
            });
            return;
          }
          
          patches.push({
            op: 'ADD',
            path: `/owners/${ownerUrn}/${ownershipTypeUrn}`,
            value: JSON.stringify({
              owner: ownerUrn,
              typeUrn: ownershipTypeUrn,
              type: 'NONE',
              source: { type: 'MANUAL' }
            })
          });
        } else {
          addError({
            entityId: entity.id,
            entityName: entity.name,
            operation: 'ownership',
            error: `Invalid ownership format: "${ownerString}". Expected format: "type:owner" or "type:owner:corpType"`,
            retryable: false
          });
        }
      });
      
      if (patches.length === 0) return null;
      
      return {
        entityType: entity.type === 'glossaryTerm' ? 'glossaryTerm' : 'glossaryNode',
        urn,
        aspectName: 'ownership',
        patch: patches
      };
    } catch (error) {
      addError({
        entityId: entity.id,
        entityName: entity.name,
        operation: 'ownership',
        error: `Failed to parse ownership: ${error instanceof Error ? error.message : 'Unknown error'}`,
        retryable: false
      });
      return null;
    }
  }, [addError]);

  const processEntityBatch = useCallback(async (batch: ImportBatch): Promise<boolean> => {
    const { entities, existingEntities } = batch;
    try {
      updateProgress({
        currentOperation: `Creating ${entities.length} entities...`,
      });

      // Load existing ownership types and create missing ones
      await ensureOwnershipTypesExist(entities);

      // Build patch inputs for all entities in the batch
      const patchInputs: EntityPatchInput[] = [];
      
      for (const entity of entities) {
        // Build patch operations for the entity
        const patches: PatchOperation[] = [];
        
        // Required fields for new entities
        patches.push({ op: 'ADD', path: '/name', value: entity.name });
        patches.push({ op: 'ADD', path: '/definition', value: entity.data.description || '' });
        patches.push({ op: 'ADD', path: '/termSource', value: entity.data.term_source || 'INTERNAL' });
        
        // Optional fields
        if (entity.data.source_ref) {
          patches.push({ op: 'ADD', path: '/sourceRef', value: entity.data.source_ref });
        }
        if (entity.data.source_url) {
          patches.push({ op: 'ADD', path: '/sourceUrl', value: entity.data.source_url });
        }
        
        // Custom properties
        if (entity.data.custom_properties) {
          try {
            const customProps = typeof entity.data.custom_properties === 'string' 
              ? JSON.parse(entity.data.custom_properties)
              : entity.data.custom_properties;
            
            Object.entries(customProps).forEach(([key, value]) => {
              patches.push({ 
                op: 'ADD', 
                path: `/customProperties/${key}`,
                value: JSON.stringify(String(value))
              });
            });
          } catch (error) {
            addError({
              entityId: entity.id,
              entityName: entity.name,
              operation: 'create',
              error: `Failed to parse custom properties: ${error instanceof Error ? error.message : 'Unknown error'}`,
              retryable: false
            });
          }
        }

        // Domain assignment - will be handled separately after entity creation

        // Ownership - will be handled separately after entity creation

        // Parent relationships - handle directly in patch operations
        if (entity.data.parent_nodes) {
          const parentNames = entity.data.parent_nodes.split(',').map(name => name.trim());
          
          for (const parentName of parentNames) {
            if (!parentName) continue;
            
            // Look up parent URN from our map or existing entities
            const parentUrn = entityUrnMap.current.get(parentName) || 
                             entity.parentUrns.find(urn => urn.includes(parentName));
            
            if (parentUrn) {
              patches.push({ 
                op: 'ADD', 
                path: '/parentNode',
                value: parentUrn
              });
            } else {
              addWarning({
                entityId: entity.id,
                entityName: entity.name,
                operation: 'parent',
                message: `Parent node "${parentName}" not found`,
              });
            }
          }
        }

        // Create the patch input for this entity
        const patchInput: EntityPatchInput = {
          entityType: entity.type === 'glossaryTerm' ? 'glossaryTerm' : 'glossaryNode',
          aspectName: entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo',
          patch: patches,
        };

        patchInputs.push(patchInput);
      }

      // Execute the GraphQL mutation for all entities in the batch
      const results = await executePatchEntitiesMutation(patchInputs);
      
      // Process results and track URNs
      let allSuccessful = true;
      const ownershipPatches: EntityPatchInput[] = [];
      
      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const entity = entities[i];
        
        if (!result.success) {
          allSuccessful = false;
          const errorMsg = result.error || 'Unknown error occurred';
          addError({
            entityId: entity.id,
            entityName: entity.name,
            operation: 'create',
            error: errorMsg,
            retryable: !errorMsg.includes('validation') && !errorMsg.includes('duplicate'),
          });
        } else {
          // Store the generated URN for relationship resolution
          const generatedUrn = result.urn;
          if (generatedUrn) {
            entityUrnMap.current.set(entity.name, generatedUrn);
            
            // Create ownership patch if ownership data exists
            if (entity.data.ownership) {
              const ownershipPatch = createOwnershipPatch(entity, generatedUrn);
              if (ownershipPatch) {
                ownershipPatches.push(ownershipPatch);
              }
            }
          }
        }
      }
      
      // Execute ownership patches if any were created
      if (ownershipPatches.length > 0) {
        try {
          updateProgress({
            currentOperation: `Setting ownership for ${ownershipPatches.length} entities...`,
          });
          
          const ownershipResults = await executePatchEntitiesMutation(ownershipPatches);
          
          // Check ownership results for errors
          for (let i = 0; i < ownershipResults.length; i++) {
            const result = ownershipResults[i];
            if (!result.success) {
              const entity = entities[i];
              addError({
                entityId: entity.id,
                entityName: entity.name,
                operation: 'ownership',
                error: result.error || 'Failed to set ownership',
                retryable: false
              });
            }
          }
        } catch (error) {
          addError({
            entityId: 'batch',
            entityName: 'Batch',
            operation: 'ownership',
            error: `Failed to set ownership: ${error instanceof Error ? error.message : 'Unknown error'}`,
            retryable: false
          });
        }
      }

      // Handle batch domain assignment
      const domainGroups = new Map<string, string[]>();
      for (const entity of entities) {
        if (entity.data.domain_urn) {
          const domainUrn = entity.data.domain_urn;
          const entityUrn = entityUrnMap.current.get(entity.name);
          if (entityUrn) {
            if (!domainGroups.has(domainUrn)) {
              domainGroups.set(domainUrn, []);
            }
            domainGroups.get(domainUrn)!.push(entityUrn);
          }
        }
      }

      // Execute batch domain assignments
      for (const [domainUrn, entityUrns] of domainGroups) {
        try {
          updateProgress({
            currentOperation: `Setting domain for ${entityUrns.length} entities...`,
          });
          
          await executeBatchSetDomainMutation(domainUrn, entityUrns);
        } catch (error) {
          addError({
            entityId: 'batch',
            entityName: 'Batch',
            operation: 'domain',
            error: `Failed to set domain: ${error instanceof Error ? error.message : 'Unknown error'}`,
            retryable: false
          });
        }
      }

      // Handle relationships after entity creation (for successful entities)
      if (allSuccessful) {
        await handleBatchRelationships(entities, existingEntities);
      }

      updateProgress({
        currentOperation: `Batch of ${entities.length} entities processed`,
      });

      return allSuccessful;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      // Add error for each entity in the batch
      entities.forEach(entity => {
        addError({
          entityId: entity.id,
          entityName: entity.name,
          operation: 'create',
          error: errorMessage,
          retryable: !errorMessage.includes('validation') && !errorMessage.includes('duplicate'),
        });
      });

      return false;
    }
  }, [executePatchEntitiesMutation, executeSetDomainMutation, executeBatchSetDomainMutation, executeGetOwnershipTypesQuery, updateProgress, addError, addWarning, createOwnershipPatch, ensureOwnershipTypesExist, handleBatchRelationships]);

  const processBatch = useCallback(async (batch: ImportBatch): Promise<void> => {
    currentBatchRef.current = batch;
    
    if (isCancelled) return;
    
    // Wait if paused
    while (isPaused && !isCancelled) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    if (isCancelled) return;

    // Process entities in batch using patchEntities mutation
    const success = await processEntityBatch(batch);
    
    // Update progress with current values
    setProgress(prevProgress => {
      const newProcessed = prevProgress.processed + batch.entities.length;
      
      const newProgress = {
        ...prevProgress,
        processed: newProcessed,
        successful: success ? prevProgress.successful + batch.entities.length : prevProgress.successful,
      };
      
      onProgress?.(newProgress);
      return newProgress;
    });
  }, [onProgress, isPaused, isCancelled]);

  const startImport = useCallback(async (entities: Entity[], existingEntities: Entity[]): Promise<void> => {
    setIsProcessing(true);
    setIsPaused(false);
    setIsCancelled(false);
    
    // Reset progress
    setProgress({
      total: entities.length,
      processed: 0,
      successful: 0,
      failed: 0,
      errors: [],
      warnings: [],
    });

    try {
      updateProgress({
        currentOperation: 'Comparing entities with existing data...',
      });

      // Compare entities with existing ones
      const comparison = categorizeEntities(entities, existingEntities);
      
      // Log comparison results
      console.log('Entity Comparison Results:', {
        new: comparison.newEntities.length,
        updated: comparison.updatedEntities.length,
        unchanged: comparison.unchangedEntities.length,
        conflicted: comparison.conflictedEntities.length
      });

      // Add warnings for conflicts
      comparison.conflictedEntities.forEach(entity => {
        addWarning({
          entityId: entity.id,
          entityName: entity.name,
          operation: 'comparison',
          message: `Conflict detected: Entity "${entity.name}" exists with different type (${entity.type} vs existing)`
        });
      });

      // Combine all entities that need processing (new + updated)
      const entitiesToProcess = [
        ...comparison.newEntities,
        ...comparison.updatedEntities
      ];

      // Skip unchanged entities
      const skippedCount = comparison.unchangedEntities.length;
      if (skippedCount > 0) {
        addWarning({
          entityId: '',
          entityName: '',
          operation: 'comparison',
          message: `${skippedCount} entities skipped (no changes detected)`
        });
      }

      // Update progress total to reflect actual entities being processed
      setProgress(prev => ({
        ...prev,
        total: entitiesToProcess.length,
        processed: 0
      }));

      if (entitiesToProcess.length === 0) {
        updateProgress({
          currentOperation: 'No entities to process',
        });
        return;
      }

      // Build hierarchy maps for entities to process
      const hierarchyMaps = {
        entitiesByLevel: new Map(),
        entitiesByName: new Map(),
        entitiesById: new Map(),
        parentChildMap: new Map(),
      };

      entitiesToProcess.forEach(entity => {
        hierarchyMaps.entitiesById.set(entity.id, entity);
        hierarchyMaps.entitiesByName.set(entity.name, entity);
        
        if (!hierarchyMaps.entitiesByLevel.has(entity.level)) {
          hierarchyMaps.entitiesByLevel.set(entity.level, []);
        }
        hierarchyMaps.entitiesByLevel.get(entity.level)!.push(entity);
      });

      // Validate hierarchy
      const hierarchyValidation = validateHierarchy(entitiesToProcess);
      if (!hierarchyValidation.isValid) {
        hierarchyValidation.errors.forEach(error => {
          addError({
            entityId: '',
            entityName: '',
            operation: 'validation',
            error: error.message,
            retryable: false
          });
        });
        return;
      }

      // Create processing batches
      const batches = createProcessingBatches(entitiesToProcess, hierarchyMaps, existingEntities);
      processingQueueRef.current = batches;

      // Process batches in order
      for (const batch of batches) {
        if (isCancelled) break;
        await processBatch(batch);
      }

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addError({
        entityId: '',
        entityName: '',
        operation: 'import',
        error: errorMessage,
        retryable: false
      });
    } finally {
      setIsProcessing(false);
      updateProgress({
        currentEntity: undefined,
        currentOperation: undefined,
      });
    }
  }, [categorizeEntities, createProcessingBatches, processBatch, validateHierarchy, addError, addWarning, updateProgress, isCancelled]);

  const pauseImport = useCallback(() => {
    setIsPaused(true);
  }, []);

  const resumeImport = useCallback(() => {
    setIsPaused(false);
  }, []);

  const cancelImport = useCallback(() => {
    setIsCancelled(true);
    setIsProcessing(false);
    setIsPaused(false);
  }, []);

  const retryFailed = useCallback(async (): Promise<void> => {
    const failedEntities = progress.errors
      .filter(error => error.retryable)
      .map(error => {
        // Find the entity by ID or name
        const entity = Array.from(processingQueueRef.current.flatMap(batch => batch.entities))
          .find(e => e.id === error.entityId || e.name === error.entityName);
        return entity;
      })
      .filter(Boolean) as Entity[];

    if (failedEntities.length === 0) return;

    // Reset failed count and errors
    setProgress(prev => ({
      ...prev,
      failed: prev.failed - failedEntities.length,
      errors: prev.errors.filter(error => !error.retryable),
    }));

    // Retry processing
    await startImport(failedEntities, []);
  }, [progress.errors, startImport]);

  const resetProgress = useCallback(() => {
    setProgress({
      total: 0,
      processed: 0,
      successful: 0,
      failed: 0,
      errors: [],
      warnings: [],
    });
    setIsProcessing(false);
    setIsPaused(false);
    setIsCancelled(false);
    processingQueueRef.current = [];
    currentBatchRef.current = null;
    retryCountRef.current.clear();
  }, []);

  return {
    progress,
    isProcessing,
    startImport,
    pauseImport,
    resumeImport,
    cancelImport,
    retryFailed,
    resetProgress,
    createProcessingBatches,
    processBatch,
  };
};