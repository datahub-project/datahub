/**
 * Hook for import processing and batch operations
 */

import { useCallback, useState } from 'react';
import { 
  Entity, 
  ImportProgress, 
  ImportError, 
  EntityPatchInput,
  UseImportProcessingReturn 
} from '../../glossary.types';

export function useImportProcessing(): UseImportProcessingReturn {
  const [progress, setProgress] = useState<ImportProgress>({
    total: 0,
    processed: 0,
    successful: 0,
    failed: 0,
    currentBatch: 0,
    totalBatches: 0,
    currentEntity: undefined,
    errors: []
  });

  /**
   * Create batches for efficient processing
   */
  const createProcessingBatches = useCallback((entities: Entity[], batchSize: number = 10): Entity[][] => {
    const batches: Entity[][] = [];
    for (let i = 0; i < entities.length; i += batchSize) {
      batches.push(entities.slice(i, i + batchSize));
    }
    return batches;
  }, []);

  /**
   * Process a batch of entities
   */
  const processUnifiedBatchOfEntities = useCallback(async (
    batch: Entity[]
  ): Promise<void> => {
    const batchNumber = progress.currentBatch + 1;
    
    // Update progress for batch start
    const updatedProgress = {
      ...progress,
      currentBatch: batchNumber,
      totalBatches: Math.ceil(progress.total / batch.length),
      currentEntity: batch[0]
    };
    setProgress(updatedProgress);

    try {
      // Create patch operations for the batch
      const patchInputs = batch.map(entity => createPatchInputForEntity(entity));
      
      // Execute GraphQL mutation (mock for now)
      const results = patchInputs.map(() => ({ success: true, error: null }));
      
      // Process results
      let successful = 0;
      let failed = 0;
      const errors: ImportError[] = [];

      results.forEach((result: any, index: number) => {
        if (result.success) {
          successful++;
        } else {
          failed++;
          errors.push({
            entity: batch[index],
            error: result.error || 'Unknown error',
            retryCount: 0,
            timestamp: new Date()
          });
        }
      });

      // Update progress
      const finalProgress = {
        ...progress,
        processed: progress.processed + batch.length,
        successful: progress.successful + successful,
        failed: progress.failed + failed,
        errors: [...progress.errors, ...errors],
        currentEntity: undefined
      };
      setProgress(finalProgress);

    } catch (error) {
      // Handle batch-level errors
      const batchError: ImportError = {
        entity: batch[0], // Use first entity as representative
        error: error instanceof Error ? error.message : 'Unknown batch error',
        retryCount: 0,
        timestamp: new Date()
      };

      const errorProgress = {
        ...progress,
        processed: progress.processed + batch.length,
        failed: progress.failed + batch.length,
        errors: [...progress.errors, batchError],
        currentEntity: undefined
      };
      setProgress(errorProgress);
    }
  }, [progress]);

  /**
   * Handle import errors with retry logic
   */
  const handleImportErrors = useCallback(async (
    error: Error, 
    entity: Entity
  ): Promise<void> => {
    const existingError = progress.errors.find(e => e.entity.id === entity.id);
    const retryCount = existingError ? existingError.retryCount + 1 : 0;

    const maxRetries = 3;
    if (retryCount < maxRetries) {
      // Retry the operation
      const patchInput = createPatchInputForEntity(entity);
      
      try {
        // Mock retry operation
        const success = Math.random() > 0.5; // Random success for demo
        
        if (success) {
          // Success on retry - remove from errors
          const updatedErrors = progress.errors.filter(e => e.entity.id !== entity.id);
          setProgress(prev => ({
            ...prev,
            successful: prev.successful + 1,
            failed: prev.failed - 1,
            errors: updatedErrors
          }));
        } else {
          // Still failed - update retry count
          const updatedErrors = progress.errors.map(e => 
            e.entity.id === entity.id 
              ? { ...e, retryCount: retryCount + 1, error: error.message }
              : e
          );
          setProgress(prev => ({
            ...prev,
            errors: updatedErrors
          }));
        }
      } catch (retryError) {
        // Retry failed - update error
        const updatedErrors = progress.errors.map(e => 
          e.entity.id === entity.id 
            ? { 
                ...e, 
                retryCount: retryCount + 1, 
                error: retryError instanceof Error ? retryError.message : 'Retry failed'
              }
            : e
        );
        setProgress(prev => ({
          ...prev,
          errors: updatedErrors
        }));
      }
    } else {
      // Max retries exceeded
      console.error(`Max retries exceeded for entity ${entity.name}:`, error);
    }
  }, [progress]);

  /**
   * Update import progress tracking
   */
  const updateProgress = useCallback((updates: Partial<ImportProgress>) => {
    setProgress(prev => ({ ...prev, ...updates }));
  }, []);

  /**
   * Reset progress for new import
   */
  const resetProgress = useCallback((totalEntities: number) => {
    setProgress({
      total: totalEntities,
      processed: 0,
      successful: 0,
      failed: 0,
      currentBatch: 0,
      totalBatches: 0,
      currentEntity: undefined,
      errors: []
    });
  }, []);

  /**
   * Get progress percentage
   */
  const getProgressPercentage = useCallback((): number => {
    if (progress.total === 0) return 0;
    return Math.round((progress.processed / progress.total) * 100);
  }, [progress]);

  /**
   * Check if import is complete
   */
  const isImportComplete = useCallback((): boolean => {
    return progress.processed >= progress.total;
  }, [progress]);

  /**
   * Get import summary
   */
  const getImportSummary = useCallback(() => {
    return {
      total: progress.total,
      processed: progress.processed,
      successful: progress.successful,
      failed: progress.failed,
      percentage: getProgressPercentage(),
      isComplete: isImportComplete(),
      errors: progress.errors
    };
  }, [progress, getProgressPercentage, isImportComplete]);

  return {
    createProcessingBatches,
    processUnifiedBatchOfEntities,
    handleImportErrors,
    updateProgress,
    resetProgress,
    getProgressPercentage,
    isImportComplete,
    getImportSummary
  } as UseImportProcessingReturn;
}

/**
 * Create patch input for an entity
 */
function createPatchInputForEntity(entity: Entity): EntityPatchInput {
  const patches: any[] = [];

  // Add basic properties patch
  patches.push({
    op: 'REPLACE',
    path: '/properties',
    value: {
      name: entity.data.name,
      description: entity.data.description,
      termSource: entity.data.term_source,
      sourceRef: entity.data.source_ref,
      sourceUrl: entity.data.source_url,
      customProperties: parseCustomProperties(entity.data.custom_properties)
    }
  });

  // Add parent nodes patch if applicable
  if (entity.parentUrns.length > 0) {
    patches.push({
      op: 'REPLACE',
      path: '/parentNodes',
      value: entity.parentUrns.map(urn => ({ urn }))
    });
  }

  // Add domain patch if applicable
  if (entity.data.domain_urn) {
    patches.push({
      op: 'REPLACE',
      path: '/domain',
      value: { urn: entity.data.domain_urn }
    });
  }

  return {
    urn: entity.urn || generateUrnForEntity(entity),
    patches
  };
}

/**
 * Parse custom properties string into array format
 */
function parseCustomProperties(customProperties: string): Array<{ key: string; value: string }> {
  if (!customProperties || customProperties.trim() === '') {
    return [];
  }

  try {
    // Try to parse as JSON first
    const parsed = JSON.parse(customProperties);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (typeof parsed === 'object') {
      return Object.entries(parsed).map(([key, value]) => ({ key, value: String(value) }));
    }
  } catch {
    // If JSON parsing fails, try key:value format
    return customProperties.split(',').map(item => {
      const [key, value] = item.split(':');
      return { key: key?.trim() || '', value: value?.trim() || '' };
    }).filter(item => item.key && item.value);
  }

  return [];
}

/**
 * Generate URN for entity if not provided
 */
function generateUrnForEntity(entity: Entity): string {
  const entityType = entity.type === 'glossaryTerm' ? 'glossaryTerm' : 'glossaryNode';
  const encodedName = encodeURIComponent(entity.name);
  return `urn:li:${entityType}:${encodedName}`;
}

/**
 * Execute patch entities mutation (placeholder - would use actual GraphQL client)
 */
async function executePatchEntitiesMutation(
  apolloClient: any, 
  input: EntityPatchInput[]
): Promise<any[]> {
  // This would be implemented using the actual GraphQL mutation
  // For now, return mock results
  return input.map(() => ({ success: true, error: null }));
}
