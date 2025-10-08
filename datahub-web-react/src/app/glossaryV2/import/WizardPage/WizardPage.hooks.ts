/**
 * Hooks for WizardPage component
 */

import { useState, useCallback } from 'react';
import { EntityData, CsvParseResult, Entity, ComparisonResult } from '../glossary.types';
import { useMockCsvProcessing } from '../shared/hooks/useMockCsvProcessing';
import { useMockEntityManagement } from '../shared/hooks/useMockEntityManagement';
import { useMockEntityComparison } from '../shared/hooks/useMockEntityComparison';
import { useHierarchyManagement } from '../shared/hooks/useHierarchyManagement';
import { useMockFileUpload, useMockFileValidation } from './DropzoneTable/useMockFileUpload';
import { mockEntities, mockExistingEntities, mockComparisonResult } from '../shared/mocks/mockData';

export function useImportState() {
  const [csvData, setCsvData] = useState<EntityData[]>([]);
  const [parseResult, setParseResult] = useState<CsvParseResult | null>(null);
  const [isDataLoaded, setIsDataLoaded] = useState(true); // Initialize as loaded for mock
  const [entities, setEntitiesState] = useState<Entity[]>(mockEntities); // Initialize with mock data
  const [existingEntities, setExistingEntitiesState] = useState<Entity[]>(mockExistingEntities); // Initialize with mock data
  const [comparisonResult, setComparisonResultState] = useState<ComparisonResult | null>(mockComparisonResult); // Initialize with mock data
  const [isComparisonComplete, setIsComparisonComplete] = useState(true); // Initialize as complete for mock

  const setCsvDataAndResult = useCallback((data: EntityData[], result: CsvParseResult) => {
    setCsvData(data);
    setParseResult(result);
    setIsDataLoaded(true);
  }, []);

  const setEntities = useCallback((entities: Entity[]) => {
    setEntitiesState(entities);
  }, []);

  const setExistingEntities = useCallback((existing: Entity[]) => {
    setExistingEntitiesState(existing);
  }, []);

  const setComparisonResult = useCallback((result: ComparisonResult) => {
    setComparisonResultState(result);
    setIsComparisonComplete(true);
  }, []);

  const clearData = useCallback(() => {
    setCsvData([]);
    setParseResult(null);
    setIsDataLoaded(false);
    setEntitiesState([]);
    setExistingEntitiesState([]);
    setComparisonResultState(null);
    setIsComparisonComplete(false);
  }, []);

  return {
    csvData,
    parseResult,
    isDataLoaded,
    entities,
    existingEntities,
    comparisonResult,
    isComparisonComplete,
    setCsvDataAndResult,
    setEntities,
    setExistingEntities,
    setComparisonResult,
    clearData
  };
}

export function useImportProgress() {
  const [progress, setProgress] = useState({
    currentStep: 0,
    totalSteps: 4,
    stepName: 'Ready to start',
    isProcessing: false,
    error: null as string | null
  });

  const updateProgress = useCallback((updates: Partial<typeof progress>) => {
    setProgress(prev => ({ ...prev, ...updates }));
  }, []);

  const resetProgress = useCallback(() => {
    setProgress({
      currentStep: 0,
      totalSteps: 4,
      stepName: 'Ready to start',
      isProcessing: false,
      error: null
    });
  }, []);

  return {
    progress,
    updateProgress,
    resetProgress
  };
}

export function useFileUploadIntegration() {
  const fileUpload = useMockFileUpload();
  const fileValidation = useMockFileValidation();
  const csvProcessing = useMockCsvProcessing();

  const handleFileSelect = useCallback(async (file: File) => {
    // Validate file first
    const validation = fileValidation.validateFile(file);
    if (!validation.isValid) {
      fileUpload.setError(validation.error!);
      return;
    }

    // Select file
    fileUpload.selectFile(file);
    fileUpload.startProcessing();

    try {
      // Read file content
      const fileContent = await readFileAsText(file);
      
      // Parse CSV
      const parseResult = csvProcessing.parseCsvText(fileContent);
      
      // Update progress
      fileUpload.updateProgress(50);
      
      // Validate parsed data
      const validationResult = csvProcessing.validateCsvData(parseResult.data);
      
      // Update progress
      fileUpload.updateProgress(100);
      
      if (!validationResult.isValid) {
        fileUpload.setError(`CSV validation failed: ${validationResult.errors.map(e => e.message).join(', ')}`);
        return;
      }

      // Complete processing
      fileUpload.completeProcessing();
      
      return {
        data: parseResult.data,
        result: parseResult,
        validation: validationResult
      };
    } catch (error) {
      fileUpload.setError(`Failed to process file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [fileUpload, fileValidation, csvProcessing]);

  const handleFileRemove = useCallback(() => {
    fileUpload.removeFile();
  }, [fileUpload]);

  return {
    uploadState: fileUpload.uploadState,
    handleFileSelect,
    handleFileRemove,
    csvProcessing
  };
}

export function useEntityComparisonIntegration() {
  const entityManagement = useMockEntityManagement();
  const entityComparison = useMockEntityComparison();
  const hierarchyManagement = useHierarchyManagement();

  const processEntities = useCallback(async (
    csvData: EntityData[],
    apolloClient: any
  ) => {
    try {
      // Step 1: Normalize CSV data to entities
      const entities = entityManagement.normalizeCsvData(csvData);
      
      // Step 2: Load existing entities from DataHub
      const existingEntities = await loadExistingEntities(apolloClient);
      const normalizedExisting = entityManagement.normalizeExistingEntities(existingEntities);
      
      // Step 3: Build hierarchy maps
      const hierarchyMaps = entityManagement.buildHierarchyMaps(entities);
      
      // Step 4: Resolve parent URNs
      const entitiesWithParents = hierarchyManagement.resolveParentUrns(entities, hierarchyMaps);
      
      // Step 5: Validate hierarchy
      const hierarchyValidation = hierarchyManagement.validateHierarchy(entitiesWithParents);
      
      // Step 6: Compare entities
      const comparisonResult = entityManagement.compareEntities(entitiesWithParents, normalizedExisting);
      
      return {
        entities: entitiesWithParents,
        existingEntities: normalizedExisting,
        comparisonResult,
        hierarchyMaps,
        hierarchyValidation
      };
    } catch (error) {
      throw new Error(`Entity processing failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [entityManagement, entityComparison, hierarchyManagement]);

  return {
    processEntities,
    entityManagement,
    entityComparison,
    hierarchyManagement
  };
}

/**
 * Load existing entities from DataHub (placeholder implementation)
 */
async function loadExistingEntities(apolloClient: any): Promise<any[]> {
  // This would use the actual GraphQL query to load existing glossary entities
  // For now, return empty array as placeholder
  return [];
}

/**
 * Helper function to read file as text
 */
function readFileAsText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      resolve(e.target?.result as string);
    };
    reader.onerror = (e) => {
      reject(new Error('Failed to read file'));
    };
    reader.readAsText(file);
  });
}
