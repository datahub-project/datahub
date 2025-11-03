/**
 * Comprehensive Import Hook
 * Handles single GraphQL call with hierarchical ordering and dependency resolution
 */

import { useState, useCallback, useRef } from 'react';
import { ApolloClient } from '@apollo/client';
import { 
  createComprehensiveImportPlan, 
  convertPlanToPatchInputs,
} from '@app/glossaryV2/import/shared/utils/comprehensiveImportUtils';
import { Entity } from '@app/glossaryV2/import/glossary.types';
import { useGraphQLOperations } from '@app/glossaryV2/import/shared/hooks/useGraphQLOperations';
import { useHierarchyManagement } from '@app/glossaryV2/import/shared/hooks/useHierarchyManagement';
import { useEntityComparison } from '@app/glossaryV2/import/shared/hooks/useEntityComparison';

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
  apolloClient?: ApolloClient<any>;
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

  const currentPlanRef = useRef<any>(null);
  const retryCountRef = useRef<Map<string, number>>(new Map());

  const { 
    executePatchEntitiesMutation,
    executeGetOwnershipTypesQuery,
    executeAddRelatedTermsMutation,
  } = useGraphQLOperations();
  
  const { validateHierarchy } = useHierarchyManagement();
  const { categorizeEntities } = useEntityComparison();

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


  const loadExistingOwnershipTypes = useCallback(async (): Promise<Map<string, string>> => {
    try {
      const result = await executeGetOwnershipTypesQuery({
        input: {
          start: 0,
          count: 1000,
        },
      });

      const ownershipTypeMap = new Map<string, string>();
      
      // GraphQL query may return array directly or wrapped in data object
      if (Array.isArray(result)) {
        const arrayResult = result;
        arrayResult.forEach((ot: any) => {
          ownershipTypeMap.set(ot.info.name.toLowerCase(), ot.urn);
        });
      } else if (result && typeof result === 'object' && 'data' in result) {
        const {data} = result as any;
        if (data?.listOwnershipTypes?.ownershipTypes) {
          data.listOwnershipTypes.ownershipTypes.forEach((ot: any) => {
            ownershipTypeMap.set(ot.info.name.toLowerCase(), ot.urn);
          });
        }
      }
      return ownershipTypeMap;
    } catch (error) {
      return new Map();
    }
  }, [executeGetOwnershipTypesQuery]);

  const executeComprehensiveImport = useCallback(async (
    allEntities: Entity[],
    existingEntities: Entity[],
    existingOwnershipTypes: Map<string, string>,
  ): Promise<void> => {
    try {
      // Use all entities for relationships, but filter to only new/updated for entity patches
      const categorizationResult = categorizeEntities(allEntities, existingEntities);
      const entitiesToProcess = [
        ...categorizationResult.newEntities,
        ...categorizationResult.updatedEntities,
      ];
      
      updateProgress({ currentPhase: 'Planning import...' });
      const plan = createComprehensiveImportPlan(allEntities, existingEntities, existingOwnershipTypes);
      currentPlanRef.current = plan;
      
      updateProgress({ currentPhase: 'Preparing patch operations...' });
      const patchInputs = convertPlanToPatchInputs(plan, entitiesToProcess, existingEntities);
      
      const regularPatches = patchInputs.filter(input => input.aspectName !== 'addRelatedTerms');
      const relationshipPatches = patchInputs.filter(input => input.aspectName === 'addRelatedTerms');
      
      // Count only entities being created/updated (not existing unchanged entities)
      const totalEntities = entitiesToProcess.length;
      const totalOwnershipTypes = plan.ownershipTypes.length;
      const totalItems = totalEntities + totalOwnershipTypes;
      
      updateProgress({ 
        currentPhase: 'Executing comprehensive import...',
        total: totalItems,
        processed: 0,
      });
      
      let results: any[] = [];
      
      if (regularPatches.length > 0) {
        const entityPatchInputs = regularPatches.map(input => ({
          urn: input.urn,
          entityType: input.entityType,
          aspectName: input.aspectName,
          patch: input.patch,
          arrayPrimaryKeys: input.arrayPrimaryKeys,
          forceGenericPatch: input.forceGenericPatch,
        }));
        
        const patchResults = await executePatchEntitiesMutation(entityPatchInputs);
        results = [...results, ...patchResults];
      }
      
      if (relationshipPatches.length > 0) {
        updateProgress({ currentPhase: 'Creating relationships...' });
        
        const relationshipResults = await Promise.all(
          relationshipPatches.map(async (relationshipPatch) => {
            try {
              const input = relationshipPatch.patch[0].value;
              await executeAddRelatedTermsMutation({
                urn: relationshipPatch.urn,
                termUrns: input.termUrns,
                relationshipType: input.relationshipType,
              });
              
              return {
                urn: relationshipPatch.urn,
                success: true,
                error: null,
              };
            } catch (error) {
              return {
                urn: relationshipPatch.urn,
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
              };
            }
          }),
        );
        results.push(...relationshipResults);
      }
      
      const failedResults = results.filter((result: any) => !result.success);
      
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
        
        errorMap.forEach((_result, errorMessage) => {
          addError({
            entityId: 'comprehensive-import',
            entityName: 'Import operation',
            operation: 'comprehensive-import',
            error: errorMessage,
            retryable: true,
          });
        });
        
        updateProgress({
          processed: totalItems,
          successful: totalItems - failedResults.length,
          failed: failedResults.length,
          currentPhase: 'Import completed with errors',
        });
      } else {
        updateProgress({
          processed: totalItems,
          successful: totalItems,
          failed: 0,
          currentPhase: 'Import completed successfully',
        });
      }
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      const currentTotal = progress.total;
      
      addError({
        entityId: 'comprehensive-import',
        entityName: 'All entities',
        operation: 'comprehensive-import',
        error: `Batch import failed: ${errorMessage}`,
        retryable: true,
      });
      
      updateProgress({
        total: currentTotal,
        processed: 0,
        successful: 0,
        failed: currentTotal,
        currentPhase: 'Import failed - all operations rolled back',
      });
    }
  }, [executePatchEntitiesMutation, executeAddRelatedTermsMutation, updateProgress, addError, categorizeEntities, progress.total]);

  const startImport = useCallback(async (entities: Entity[], existingEntities: Entity[]): Promise<void> => {
    if (isProcessing) {
      return;
    }
    
    setIsProcessing(true);
    setIsPaused(false);
    setIsCancelled(false);
    
    updateProgress({
      total: 0,
      processed: 0,
      successful: 0,
      failed: 0,
      errors: [],
      warnings: [],
      currentPhase: 'Initializing...',
    });

    try {
      updateProgress({ currentPhase: 'Categorizing entities...' });
      const categorizationResult = categorizeEntities(entities, existingEntities);
      
      const entitiesToProcess = [
        ...categorizationResult.newEntities,
        ...categorizationResult.updatedEntities,
      ];
      
      if (entitiesToProcess.length === 0) {
        updateProgress({
          currentPhase: 'No entities to process',
          total: 0,
          processed: 0,
        });
        return;
      }

      updateProgress({ currentPhase: 'Validating hierarchy...' });
      const hierarchyValidation = validateHierarchy(entitiesToProcess);
      if (!hierarchyValidation.isValid) {
        hierarchyValidation.errors.forEach(error => {
          addError({
            entityId: '',
            entityName: '',
            operation: 'validation',
            error: error.message,
            retryable: false,
          });
        });
        return;
      }

      updateProgress({ currentPhase: 'Loading existing ownership types...' });
      const existingOwnershipTypes = await loadExistingOwnershipTypes();

      // Pass all entities for relationship processing, not just new/updated
      await executeComprehensiveImport(entities, existingEntities, existingOwnershipTypes);

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      addError({
        entityId: '',
        entityName: '',
        operation: 'import',
        error: errorMessage,
        retryable: false,
      });
      
      updateProgress({
        currentPhase: 'Import failed',
        failed: progress.failed + 1,
      });
    } finally {
      setIsProcessing(false);
      updateProgress({
        currentPhase: undefined,
        currentEntity: undefined,
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
    progress.failed,
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
             return;
           }
    
    setIsCancelled(false);
    setIsPaused(false);
    
    const hasRetryableErrors = progress.errors.some(error => error.retryable);
    if (!hasRetryableErrors) {
      return;
    }

    // Since the mutation is atomic, we need to retry the entire batch
    // This is a limitation of the atomic nature - we can't retry individual entities
    // For now, we'll just reset and let the user manually retry
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
