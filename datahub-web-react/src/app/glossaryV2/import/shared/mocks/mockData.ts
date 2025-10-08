import { Entity, EntityData, ImportProgress, ComparisonResult } from '../../glossary.types';

// Mock CSV data
export const mockCsvData: EntityData[] = [
  {
    entity_type: 'glossaryTerm',
    urn: '',
    name: 'Customer ID',
    description: 'Unique identifier for each customer',
    term_source: 'INTERNAL',
    source_ref: '',
    source_url: '',
    ownership_users: 'admin:DEVELOPER',
    ownership_groups: 'bfoo:Technical Owner',
    parent_nodes: 'Business Terms',
    related_contains: '',
    related_inherits: '',
    domain_urn: '',
    domain_name: '',
    custom_properties: 'sensitivity:high,classification:internal'
  },
  {
    entity_type: 'glossaryTerm',
    urn: '',
    name: 'Customer Name',
    description: 'Full name of the customer',
    term_source: 'INTERNAL',
    source_ref: '',
    source_url: '',
    ownership_users: 'datahub:Technical Owner',
    ownership_groups: '',
    parent_nodes: 'Business Terms',
    related_contains: '',
    related_inherits: '',
    domain_urn: '',
    domain_name: '',
    custom_properties: 'sensitivity:medium'
  },
  {
    entity_type: 'glossaryNode',
    urn: '',
    name: 'Business Terms',
    description: 'Core business terminology',
    term_source: 'INTERNAL',
    source_ref: '',
    source_url: '',
    ownership_users: 'admin:DEVELOPER',
    ownership_groups: 'bfoo:Business Owner',
    parent_nodes: '',
    related_contains: '',
    related_inherits: '',
    domain_urn: '',
    domain_name: '',
    custom_properties: ''
  },
  {
    entity_type: 'glossaryTerm',
    urn: '',
    name: 'Email Address',
    description: 'Customer email address for communication',
    term_source: 'INTERNAL',
    source_ref: '',
    source_url: '',
    ownership_users: 'datahub:Technical Owner',
    ownership_groups: 'bfoo:Technical Owner',
    parent_nodes: 'Business Terms',
    related_contains: '',
    related_inherits: '',
    domain_urn: '',
    domain_name: '',
    custom_properties: 'sensitivity:high,classification:internal'
  }
];

// Mock existing entities
export const mockExistingEntities: Entity[] = [
  {
    id: 'existing-1',
    name: 'Customer ID',
    type: 'glossaryTerm',
    urn: 'urn:li:glossaryTerm:existing-customer-id',
    parentNames: ['Business Terms'],
    parentUrns: ['urn:li:glossaryNode:business-terms'],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: 'urn:li:glossaryTerm:existing-customer-id',
      name: 'Customer ID',
      description: 'Legacy customer identifier',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: '',
      parent_nodes: 'Business Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:high'
    },
    status: 'existing'
  },
  {
    id: 'existing-2',
    name: 'Business Terms',
    type: 'glossaryNode',
    urn: 'urn:li:glossaryNode:business-terms',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: {
      entity_type: 'glossaryNode',
      urn: 'urn:li:glossaryNode:business-terms',
      name: 'Business Terms',
      description: 'Core business terminology',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Business Owner',
      parent_nodes: '',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: ''
    },
    status: 'existing'
  }
];

// Mock entities with different statuses
export const mockEntities: Entity[] = [
  {
    id: '1',
    name: 'Customer ID',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Customer ID',
      description: 'Unique identifier for each customer',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Business Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:high,classification:internal'
    },
    status: 'updated'
  },
  {
    id: '2',
    name: 'Customer Name',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Customer Name',
      description: 'Full name of the customer',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: '',
      parent_nodes: 'Business Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:medium'
    },
    status: 'new'
  },
  {
    id: '3',
    name: 'Business Terms',
    type: 'glossaryNode',
    urn: 'urn:li:glossaryNode:business-terms',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: {
      entity_type: 'glossaryNode',
      urn: 'urn:li:glossaryNode:business-terms',
      name: 'Business Terms',
      description: 'Core business terminology',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Business Owner',
      parent_nodes: '',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: ''
    },
    status: 'existing'
  },
  {
    id: '4',
    name: 'Email Address',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Email Address',
      description: 'Customer email address for communication',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Business Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:high,classification:internal'
    },
    status: 'conflict'
  }
];

// Mock comparison result
export const mockComparisonResult: ComparisonResult = {
  newEntities: [mockEntities[1]], // Customer Name
  updatedEntities: [mockEntities[0]], // Customer ID
  unchangedEntities: [mockEntities[2]], // Business Terms
  conflictedEntities: [mockEntities[3]], // Email Address
  totalEntities: 4,
  newCount: 1,
  updatedCount: 1,
  unchangedCount: 1,
  conflictedCount: 1
};

// Mock progress states
export const mockProgressStates = {
  idle: {
    isProcessing: false,
    currentOperation: '',
    processed: 0,
    total: 0,
    errors: [],
    warnings: [],
    success: false
  } as ImportProgress,
  
  processing: {
    isProcessing: true,
    currentOperation: 'Processing entities...',
    processed: 2,
    total: 4,
    errors: [],
    warnings: [],
    success: false
  } as ImportProgress,
  
  completed: {
    isProcessing: false,
    currentOperation: 'Import completed successfully',
    processed: 4,
    total: 4,
    errors: [],
    warnings: [],
    success: true
  } as ImportProgress,
  
  withErrors: {
    isProcessing: false,
    currentOperation: 'Import completed with errors',
    processed: 3,
    total: 4,
    errors: [
      {
        entityId: '4',
        entityName: 'Email Address',
        operation: 'create',
        error: 'Validation failed: Invalid email format',
        retryable: true
      }
    ],
    warnings: [
      {
        entityId: '2',
        entityName: 'Customer Name',
        operation: 'ownership',
        message: 'Auto-discovered 1 parent relationship(s) from existing entities'
      }
    ],
    success: false
  } as ImportProgress
};

// Mock file upload states
export const mockFileUploadStates = {
  idle: {
    isDragOver: false,
    isUploading: false,
    isProcessing: false,
    isComplete: false,
    progress: 0,
    error: null,
    file: null
  },
  
  uploading: {
    isDragOver: false,
    isUploading: true,
    isProcessing: false,
    isComplete: false,
    progress: 50,
    error: null,
    file: new File(['mock content'], 'test.csv', { type: 'text/csv' })
  },
  
  processing: {
    isDragOver: false,
    isUploading: false,
    isProcessing: true,
    isComplete: false,
    progress: 75,
    error: null,
    file: new File(['mock content'], 'test.csv', { type: 'text/csv' })
  },
  
  complete: {
    isDragOver: false,
    isUploading: false,
    isProcessing: false,
    isComplete: true,
    progress: 100,
    error: null,
    file: new File(['mock content'], 'test.csv', { type: 'text/csv' })
  },
  
  error: {
    isDragOver: false,
    isUploading: false,
    isProcessing: false,
    isComplete: false,
    progress: 0,
    error: 'Invalid CSV format: Missing required columns',
    file: null
  }
};
