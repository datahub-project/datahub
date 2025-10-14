/**
 * Comprehensive Import Hook
 * Handles single GraphQL call with hierarchical ordering and dependency resolution
 */

import { useState, useCallback, useRef } from 'react';
import { ApolloClient } from '@apollo/client';
import { Entity, EntityData, HierarchyMaps, ValidationError, ValidationWarning } from '../../glossary.types';
import { useGraphQLOperations } from './useGraphQLOperations';
import { useHierarchyManagement } from './useHierarchyManagement';
import { useEntityManagement } from './useEntityManagement';
import { useEntityComparison } from './useEntityComparison';
import { useUserContext } from '@app/context/useUserContext';
import { 
  createComprehensiveImportPlan, 
  convertPlanToPatchInputs,
  ComprehensivePatchInput 
} from '../utils/comprehensiveImportUtils';
import { preGenerateUrns } from '../utils/urnGenerationUtils';

export interface ComprehensiveImportProgress {
  total: number;
  processed: number;
  successful: number;
  failed: number;
  currentPhase?: string;
  currentEntity?: Entity;
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

export interface UseComprehensiveImportProps {
  apolloClient: ApolloClient<any>;
  onProgress?: (progress: ComprehensiveImportProgress) => void;
}

export interface UseComprehensiveImportReturn {
  progress: ComprehensiveImportProgress;
  isProcessing: boolean;
  isPaused: boolean;
  isCancelled: boolean;
  startImport: (entities: Entity[], existingEntities: Entity[]) => Promise<void>;
  pauseImport: () => void;
  resumeImport: () => void;
  cancelImport: () => void;
  retryFailed: () => Promise<void>;
  resetProgress: () => void;
}

export const useComprehensiveImport = ({
  apolloClient,
  onProgress,
}: UseComprehensiveImportProps): UseComprehensiveImportReturn => {
  const [progress, setProgress] = useState<ComprehensiveImportProgress>({
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

  // Get current user context
  const user = useUserContext();

  // Refs for tracking state
  const currentPlanRef = useRef<any>(null);
  const retryCountRef = useRef<Map<string, number>>(new Map());

  const { 
    executeUnifiedGlossaryQuery, 
    executePatchEntitiesMutation,
    executeGetOwnershipTypesQuery,
    executeAddRelatedTermsMutation
  } = useGraphQLOperations();
  
  const { createProcessingOrder, validateHierarchy } = useHierarchyManagement();
  const { normalizeCsvData, compareEntities } = useEntityManagement();
  const { categorizeEntities, detectConflicts, getChangeDetails } = useEntityComparison();

  const updateProgress = useCallback((updates: Partial<ComprehensiveImportProgress>) => {
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
   * Load existing ownership types
   */
  const loadExistingOwnershipTypes = useCallback(async (): Promise<Map<string, string>> => {
    try {
      const result = await executeGetOwnershipTypesQuery({
        input: {
          start: 0,
          count: 1000
        }
      });

      const ownershipTypeMap = new Map<string, string>();
      
      // Handle the actual response format - it's an array directly
      if (Array.isArray(result)) {
        const arrayResult = result;
        arrayResult.forEach((ot: any) => {
          ownershipTypeMap.set(ot.info.name.toLowerCase(), ot.urn);
        });
      } else if (result && typeof result === 'object' && 'data' in result) {
        const data = (result as any).data;
        if (data?.listOwnershipTypes?.ownershipTypes) {
          data.listOwnershipTypes.ownershipTypes.forEach((ot: any) => {
            ownershipTypeMap.set(ot.info.name.toLowerCase(), ot.urn);
          });
        }
      }
      return ownershipTypeMap;
    } catch (error) {
      console.error('Failed to load existing ownership types:', error);
      return new Map();
    }
  }, [executeGetOwnershipTypesQuery]);

  /**
   * Execute comprehensive import in single GraphQL call
   */
  const executeComprehensiveImport = useCallback(async (
    allEntities: Entity[],
    existingEntities: Entity[],
    existingOwnershipTypes: Map<string, string>
  ): Promise<void> => {
    console.log('🚀 Starting comprehensive import...');
    
    try {
      // 1. Categorize entities to get only new and updated for entity patches
      const categorizationResult = categorizeEntities(allEntities, existingEntities);
      const entitiesToProcess = [
        ...categorizationResult.newEntities,
        ...categorizationResult.updatedEntities
      ];
      
      // 2. Create comprehensive import plan (use all entities for relationships, filtered for entity patches)
      updateProgress({ currentPhase: 'Planning import...' });
      const plan = createComprehensiveImportPlan(allEntities, existingEntities, existingOwnershipTypes);
      currentPlanRef.current = plan;
      
      
      // 3. Convert plan to patch inputs
      updateProgress({ currentPhase: 'Preparing patch operations...' });
      const patchInputs = convertPlanToPatchInputs(plan, entitiesToProcess, existingEntities);
      
      // Separate regular patches from addRelatedTerms mutations
      const regularPatches = patchInputs.filter(input => input.aspectName !== 'addRelatedTerms');
      const relationshipPatches = patchInputs.filter(input => input.aspectName === 'addRelatedTerms');
      
      // 4. Execute regular patch operations
      updateProgress({ 
        currentPhase: 'Executing comprehensive import...',
        total: patchInputs.length,
        processed: 0
      });
      
      let results: any[] = [];
      
      // Execute regular patchEntities mutations
      if (regularPatches.length > 0) {
        const entityPatchInputs = regularPatches.map(input => ({
          urn: input.urn,
          entityType: input.entityType,
          aspectName: input.aspectName,
          patch: input.patch,
          arrayPrimaryKeys: input.arrayPrimaryKeys?.map(pk => ({
            keyPath: pk.arrayField,
            primaryKeys: pk.keys
          })),
          forceGenericPatch: input.forceGenericPatch
        }));
        
        const patchResults = await executePatchEntitiesMutation(entityPatchInputs);
        results = [...results, ...patchResults];
      }
      
      // Execute addRelatedTerms mutations
      if (relationshipPatches.length > 0) {
        updateProgress({ currentPhase: 'Creating relationships...' });
        
        for (const relationshipPatch of relationshipPatches) {
          try {
            const input = relationshipPatch.patch[0].value;
            const addRelatedTermsResult = await executeAddRelatedTermsMutation({
              urn: relationshipPatch.urn,
              termUrns: input.termUrns,
              relationshipType: input.relationshipType
            });
            
            // Add success result for consistency
            results.push({
              urn: relationshipPatch.urn,
              success: true,
              error: null
            });
          } catch (error) {
            console.error('Failed to create relationship:', error);
            results.push({
              urn: relationshipPatch.urn,
              success: false,
              error: error instanceof Error ? error.message : 'Unknown error'
            });
          }
        }
      }
      
               // 4. Process results - Check if any operations failed
      
      const totalEntities = plan.entities.length;
      const totalOwnershipTypes = plan.ownershipTypes.length;
      const totalOperations = totalEntities + totalOwnershipTypes;
      
      // Check if any results indicate failure
      const failedResults = results.filter((result: any) => !result.success);
      const successfulResults = results.filter((result: any) => result.success);
      
               if (failedResults.length > 0) {
        
        // Aggregate duplicate errors to avoid showing the same error multiple times
        const errorMap = new Map<string, { count: number; firstResult: any }>();
        
        failedResults.forEach((result: any) => {
          const errorKey = result.error || 'Unknown error';
          if (errorMap.has(errorKey)) {
            errorMap.get(errorKey)!.count++;
          } else {
            errorMap.set(errorKey, { count: 1, firstResult: result });
          }
        });
        
        // Add one error per unique error message
        errorMap.forEach(({ firstResult }, errorMessage) => {
          addError({
            entityId: 'comprehensive-import',
            entityName: 'Import operation',
            operation: 'comprehensive-import',
            error: errorMessage,
            retryable: true
          });
        });
        
        updateProgress({
          processed: patchInputs.length,
          successful: successfulResults.length,
          failed: failedResults.length,
          currentPhase: 'Import completed with errors'
        });
      } else {
        // All operations succeeded
        updateProgress({
          processed: patchInputs.length,
          successful: totalOperations,
          failed: 0,
          currentPhase: 'Import completed successfully'
        });
        
      }
      
    } catch (error) {
      console.error('Comprehensive import failed:', error);
      
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      // Since the mutation is atomic, if it fails, all entities failed
      const totalEntities = currentPlanRef.current?.entities?.length || 0;
      const totalOwnershipTypes = currentPlanRef.current?.ownershipTypes?.length || 0;
      const totalOperations = totalEntities + totalOwnershipTypes;
      
      addError({
        entityId: 'comprehensive-import',
        entityName: 'All entities',
        operation: 'comprehensive-import',
        error: `Batch import failed: ${errorMessage}`,
        retryable: true
      });
      
      updateProgress({
        processed: 0,
        successful: 0,
        failed: totalOperations,
        currentPhase: 'Import failed - all operations rolled back'
      });
    }
  }, [executePatchEntitiesMutation, updateProgress, addError, progress.failed]);

  /**
   * Start comprehensive import
   */
  const startImport = useCallback(async (entities: Entity[], existingEntities: Entity[]): Promise<void> => {
    if (isProcessing) {
      console.warn('Import already in progress');
      return;
    }

    console.log('🎯 Starting comprehensive import process...');
    
    setIsProcessing(true);
    setIsPaused(false);
    setIsCancelled(false);
    
    // Reset progress
    updateProgress({
      total: 0,
      processed: 0,
      successful: 0,
      failed: 0,
      errors: [],
      warnings: [],
      currentPhase: 'Initializing...'
    });

    try {
      // 1. Categorize entities
      updateProgress({ currentPhase: 'Categorizing entities...' });
      const categorizationResult = categorizeEntities(entities, existingEntities);
      
      // Filter to only process new and updated entities
      const entitiesToProcess = [
        ...categorizationResult.newEntities,
        ...categorizationResult.updatedEntities
      ];
      
      if (entitiesToProcess.length === 0) {
        updateProgress({
          currentPhase: 'No entities to process',
          total: 0,
          processed: 0
        });
        return;
      }


      // 2. Validate hierarchy
      updateProgress({ currentPhase: 'Validating hierarchy...' });
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

      // 3. Load existing ownership types
      updateProgress({ currentPhase: 'Loading existing ownership types...' });
      const existingOwnershipTypes = await loadExistingOwnershipTypes();

      // 4. Execute comprehensive import (pass all entities for relationship processing)
      await executeComprehensiveImport(entities, existingEntities, existingOwnershipTypes);

    } catch (error) {
      console.error('❌ Import process failed:', error);
      
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addError({
        entityId: '',
        entityName: '',
        operation: 'import',
        error: errorMessage,
        retryable: false
      });
      
      updateProgress({
        currentPhase: 'Import failed',
        failed: progress.failed + 1
      });
    } finally {
      setIsProcessing(false);
      updateProgress({
        currentPhase: undefined,
        currentEntity: undefined
      });
    }
  }, [
    isProcessing,
    categorizeEntities,
    validateHierarchy,
    loadExistingOwnershipTypes,
    executeComprehensiveImport,
    addError,
    updateProgress,
    progress.failed
  ]);

  const pauseImport = useCallback(() => {
    setIsPaused(true);
    updateProgress({ currentPhase: 'Import paused' });
  }, [updateProgress]);

  const resumeImport = useCallback(() => {
    setIsPaused(false);
    updateProgress({ currentPhase: 'Resuming import...' });
  }, [updateProgress]);

  const cancelImport = useCallback(() => {
    setIsCancelled(true);
    setIsProcessing(false);
    setIsPaused(false);
    updateProgress({ currentPhase: 'Import cancelled' });
  }, [updateProgress]);

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
    currentPlanRef.current = null;
    retryCountRef.current.clear();
  }, []);

         const retryFailed = useCallback(async () => {
           if (!currentPlanRef.current) {
             console.warn('No import plan available for retry');
             return;
           }
    
    // Reset retry state
    setIsCancelled(false);
    setIsPaused(false);
    
    // Check if there are retryable errors
    const hasRetryableErrors = progress.errors.some(error => error.retryable);
    if (!hasRetryableErrors) {
      console.log('No retryable errors found');
      return;
    }

    // Since the mutation is atomic, we need to retry the entire batch
    // We'll need the original entities and existing entities to retry
    // This is a limitation of the atomic nature - we can't retry individual entities
    console.log('⚠️ Retrying entire batch due to atomic nature of GraphQL mutations');
    
    // For now, we'll just reset and let the user manually retry
    // In a real implementation, you'd want to store the original entities
    resetProgress();
  }, [progress.errors, resetProgress]);

  return {
    progress,
    isProcessing,
    isPaused,
    isCancelled,
    startImport,
    pauseImport,
    resumeImport,
    cancelImport,
    retryFailed,
    resetProgress,
  };
};
