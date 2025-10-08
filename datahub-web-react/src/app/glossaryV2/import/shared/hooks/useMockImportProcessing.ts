import { useState, useCallback } from 'react';
import { ImportProgress, Entity, ComparisonResult } from '../../glossary.types';
import { 
  mockEntities, 
  mockExistingEntities, 
  mockComparisonResult, 
  mockProgressStates 
} from '../mocks/mockData';

export interface UseMockImportProcessingProps {
  apolloClient?: any;
  onProgress?: (progress: ImportProgress) => void;
  batchSize?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export interface UseMockImportProcessingReturn {
  progress: ImportProgress;
  isProcessing: boolean;
  startImport: (entities: Entity[], existingEntities: Entity[]) => Promise<void>;
  pauseImport: () => void;
  resumeImport: () => void;
  cancelImport: () => void;
  retryFailed: () => Promise<void>;
  resetProgress: () => void;
  createProcessingBatches: (entities: Entity[], existingEntities: Entity[]) => any[];
  processBatch: (batch: any) => Promise<void>;
}

export const useMockImportProcessing = ({
  apolloClient,
  onProgress,
  batchSize = 50,
  maxRetries = 3,
  retryDelay = 1000,
}: UseMockImportProcessingProps = {}): UseMockImportProcessingReturn => {
  const [progress, setProgress] = useState<ImportProgress>(mockProgressStates.idle);
  const [isProcessing, setIsProcessing] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [isCancelled, setIsCancelled] = useState(false);

  const updateProgress = useCallback((updates: Partial<ImportProgress>) => {
    setProgress(prev => {
      const newProgress = { ...prev, ...updates };
      onProgress?.(newProgress);
      return newProgress;
    });
  }, [onProgress]);

  const startImport = useCallback(async (entities: Entity[], existingEntities: Entity[]) => {
    setIsProcessing(true);
    setIsPaused(false);
    setIsCancelled(false);
    
    // Simulate import process
    updateProgress({
      ...mockProgressStates.processing,
      total: entities.length
    });

    // Simulate processing delay
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Simulate completion
    updateProgress(mockProgressStates.completed);
    setIsProcessing(false);
  }, [updateProgress]);

  const pauseImport = useCallback(() => {
    setIsPaused(true);
    updateProgress({
      ...progress,
      currentOperation: 'Import paused'
    });
  }, [progress, updateProgress]);

  const resumeImport = useCallback(() => {
    setIsPaused(false);
    updateProgress({
      ...progress,
      currentOperation: 'Resuming import...'
    });
  }, [progress, updateProgress]);

  const cancelImport = useCallback(() => {
    setIsCancelled(true);
    setIsProcessing(false);
    updateProgress({
      ...progress,
      isProcessing: false,
      currentOperation: 'Import cancelled'
    });
  }, [progress, updateProgress]);

  const retryFailed = useCallback(async () => {
    setIsProcessing(true);
    updateProgress({
      ...mockProgressStates.processing,
      currentOperation: 'Retrying failed entities...'
    });

    // Simulate retry delay
    await new Promise(resolve => setTimeout(resolve, 1500));

    updateProgress(mockProgressStates.completed);
    setIsProcessing(false);
  }, [updateProgress]);

  const resetProgress = useCallback(() => {
    setProgress(mockProgressStates.idle);
    setIsProcessing(false);
    setIsPaused(false);
    setIsCancelled(false);
  }, []);

  const createProcessingBatches = useCallback((entities: Entity[], existingEntities: Entity[]) => {
    // Return mock batch data
    return [{
      entities: mockEntities,
      existingEntities: mockExistingEntities,
      hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
    }];
  }, []);

  const processBatch = useCallback(async (batch: any) => {
    // Mock batch processing
    await new Promise(resolve => setTimeout(resolve, 1000));
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
