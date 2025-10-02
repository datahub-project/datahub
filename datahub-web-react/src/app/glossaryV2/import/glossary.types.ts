/**
 * TypeScript interfaces for Glossary Import feature
 */

// Raw CSV data structure
export interface EntityData {
  entity_type: 'glossaryTerm' | 'glossaryNode';
  urn: string;
  name: string;
  description: string;
  term_source: string;
  source_ref: string;
  source_url: string;
  ownership: string;
  parent_nodes: string;
  related_contains: string;
  related_inherits: string;
  domain_urn: string;
  domain_name: string;
  custom_properties: string;
  status?: string;
}

// Processed entity structure
export interface Entity {
  id: string;                    // Unique identifier (hierarchy path)
  name: string;                  // Display name
  type: 'glossaryTerm' | 'glossaryNode';
  urn?: string;                  // URN if exists
  parentNames: string[];         // Array of parent names
  parentUrns: string[];          // Array of parent URNs (resolved)
  level: number;                 // Hierarchy level (0 = root)
  data: EntityData;              // All other entity data
  status: 'existing' | 'new' | 'updated' | 'conflict';
  originalRow?: EntityData;      // Reference to original CSV row
}

// Hierarchy management maps
export interface HierarchyMaps {
  entitiesByLevel: Map<number, Entity[]>;
  entitiesByName: Map<string, Entity>;
  entitiesById: Map<string, Entity>;
  parentChildMap: Map<string, string[]>; // parent name -> children names
}

// Entity comparison result
export interface ComparisonResult {
  newEntities: Entity[];
  existingEntities: Entity[];
  updatedEntities: Entity[];
  conflicts: Entity[];
}

// CSV parsing result
export interface CsvParseResult {
  data: EntityData[];
  errors: CsvError[];
  warnings: CsvWarning[];
}

// CSV parsing error
export interface CsvError {
  row: number;
  field?: string;
  message: string;
  type: 'required' | 'format' | 'validation' | 'duplicate';
}

// CSV parsing warning
export interface CsvWarning {
  row: number;
  field?: string;
  message: string;
  type: 'missing' | 'format' | 'suggestion';
}

// Import progress tracking
export interface ImportProgress {
  total: number;
  processed: number;
  successful: number;
  failed: number;
  currentBatch: number;
  totalBatches: number;
  currentEntity?: Entity;
  errors: ImportError[];
}

// Import error
export interface ImportError {
  entity: Entity;
  error: string;
  retryCount: number;
  timestamp: Date;
}

// GraphQL entity from DataHub
export interface GraphQLEntity {
  __typename: 'GlossaryTerm' | 'GlossaryNode';
  urn: string;
  name?: string;
  hierarchicalName?: string;
  properties?: {
    name: string;
    description?: string;
    termSource?: string;
    sourceRef?: string;
    sourceUrl?: string;
    customProperties?: Array<{
      key: string;
      value: string;
    }>;
  };
  parentNodes?: {
    nodes: Array<{
      urn: string;
      properties: {
        name: string;
      };
    }>;
  };
  ownership?: {
    owners: Array<{
      owner: {
        __typename: string;
        urn: string;
        username?: string;
        name?: string;
        info?: {
          displayName?: string;
          email?: string;
          firstName?: string;
          lastName?: string;
          fullName?: string;
          description?: string;
        };
      };
      type: string;
      ownershipType?: {
        urn: string;
        info: {
          name: string;
          description?: string;
        };
      };
    }>;
  };
  domain?: {
    domain: {
      urn: string;
      properties: {
        name: string;
        description?: string;
      };
    };
  };
}

// Patch operation for GraphQL mutations
export interface PatchOperation {
  op: 'ADD' | 'REMOVE' | 'REPLACE' | 'MOVE' | 'COPY' | 'TEST';
  path: string;
  value?: any;
  from?: string;
}

// Entity patch input for GraphQL mutations
export interface EntityPatchInput {
  urn: string;
  patches: PatchOperation[];
}

// File upload state
export interface FileUploadState {
  file: File | null;
  isUploading: boolean;
  isProcessing: boolean;
  progress: number;
  error: string | null;
}

// Grid state
export interface GridState {
  selectedEntity: Entity | null;
  sortColumn: string;
  sortDirection: 'asc' | 'desc';
  filterStatus: string[];
  filterType: string[];
  searchTerm: string;
  currentPage: number;
  pageSize: number;
}

// Validation result
export interface ValidationResult {
  isValid: boolean;
  errors: ValidationError[];
  warnings: ValidationWarning[];
}

// Validation error
export interface ValidationError {
  field: string;
  message: string;
  code: string;
}

// Validation warning
export interface ValidationWarning {
  field: string;
  message: string;
  code: string;
}

// Hook return types
export interface UseCsvProcessingReturn {
  parseCsvText: (csvText: string) => CsvParseResult;
  validateCsvData: (data: EntityData[]) => ValidationResult;
  normalizeCsvRow: (row: any) => EntityData;
  toCsvString: (data: EntityData[]) => string;
  createEmptyRow: () => EntityData;
}

export interface UseEntityManagementReturn {
  normalizeCsvData: (data: EntityData[]) => Entity[];
  normalizeExistingEntities: (entities: GraphQLEntity[]) => Entity[];
  compareEntities: (imported: Entity[], existing: Entity[]) => ComparisonResult;
  buildHierarchyMaps: (entities: Entity[]) => HierarchyMaps;
  validateEntity: (entity: Entity) => ValidationResult;
}

export interface UseHierarchyManagementReturn {
  createProcessingOrder: (entities: Entity[]) => Entity[];
  resolveParentUrns: (entities: Entity[], hierarchyMaps: HierarchyMaps) => Entity[];
  resolveParentUrnsForLevel: (entities: Entity[], level: number, hierarchyMaps: HierarchyMaps) => Entity[];
  validateHierarchy: (entities: Entity[]) => ValidationResult;
}

export interface UseEntityComparisonReturn {
  compareEntityData: (entity1: EntityData, entity2: EntityData) => boolean;
  identifyChanges: (entity1: EntityData, entity2: EntityData) => string[];
  detectConflicts: (entity1: Entity, entity2: Entity) => boolean;
  categorizeEntities: (imported: Entity[], existing: Entity[]) => {
    newEntities: Entity[];
    updatedEntities: Entity[];
    unchangedEntities: Entity[];
    conflictedEntities: Entity[];
  };
  getChangeDetails: (imported: Entity, existing: Entity) => {
    hasChanges: boolean;
    changedFields: string[];
    changeSummary: string;
  };
  validateEntityCompatibility: (imported: Entity, existing: Entity) => {
    isCompatible: boolean;
    issues: string[];
  };
}

export interface UseImportProcessingReturn {
  createProcessingBatches: (entities: Entity[], batchSize: number) => Entity[][];
  processUnifiedBatchOfEntities: (batch: Entity[]) => Promise<void>;
  handleImportErrors: (error: Error, entity: Entity) => Promise<void>;
  updateProgress: (progress: Partial<ImportProgress>) => void;
  resetProgress: (totalEntities: number) => void;
  getProgressPercentage: () => number;
  isImportComplete: () => boolean;
  getImportSummary: () => {
    total: number;
    processed: number;
    successful: number;
    failed: number;
    percentage: number;
    isComplete: boolean;
    errors: ImportError[];
  };
}

export interface UseGraphQLOperationsReturn {
  executeUnifiedGlossaryQuery: (variables: any) => Promise<GraphQLEntity[]>;
  executePatchEntitiesMutation: (input: EntityPatchInput[]) => Promise<any>;
  handleGraphQLErrors: (error: any) => string;
}
