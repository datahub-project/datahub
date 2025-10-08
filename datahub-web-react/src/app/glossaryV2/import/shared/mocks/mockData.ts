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
  },
  {
    id: '5',
    name: 'Phone Number',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Phone Number',
      description: 'Customer contact phone number',
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
      custom_properties: 'sensitivity:high,classification:internal'
    },
    status: 'new'
  },
  {
    id: '6',
    name: 'Address',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Address',
      description: 'Customer physical address',
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
      custom_properties: 'sensitivity:high,classification:confidential'
    },
    status: 'updated'
  },
  {
    id: '7',
    name: 'Date of Birth',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Business Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Date of Birth',
      description: 'Customer birth date for age verification',
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
      custom_properties: 'sensitivity:high,classification:confidential'
    },
    status: 'new'
  },
  {
    id: '8',
    name: 'Product Catalog',
    type: 'glossaryNode',
    urn: '',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: {
      entity_type: 'glossaryNode',
      urn: '',
      name: 'Product Catalog',
      description: 'Product-related terminology and definitions',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: '',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'category:product,department:sales'
    },
    status: 'existing'
  },
  {
    id: '9',
    name: 'Product ID',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Product Catalog'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Product ID',
      description: 'Unique identifier for each product',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: '',
      parent_nodes: 'Product Catalog',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:low,classification:public'
    },
    status: 'new'
  },
  {
    id: '10',
    name: 'Product Name',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Product Catalog'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Product Name',
      description: 'Human-readable name of the product',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Product Catalog',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:low,classification:public'
    },
    status: 'updated'
  },
  {
    id: '11',
    name: 'Product Price',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Product Catalog'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Product Price',
      description: 'Current selling price of the product',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: '',
      parent_nodes: 'Product Catalog',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:medium,classification:internal'
    },
    status: 'conflict'
  },
  {
    id: '12',
    name: 'Order Management',
    type: 'glossaryNode',
    urn: '',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: {
      entity_type: 'glossaryNode',
      urn: '',
      name: 'Order Management',
      description: 'Order processing and fulfillment terminology',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: '',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'category:order,department:operations'
    },
    status: 'existing'
  },
  {
    id: '13',
    name: 'Order ID',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Order Management'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Order ID',
      description: 'Unique identifier for each order',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: '',
      parent_nodes: 'Order Management',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:low,classification:public'
    },
    status: 'new'
  },
  {
    id: '14',
    name: 'Order Status',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Order Management'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Order Status',
      description: 'Current status of the order (pending, shipped, delivered)',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Order Management',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:low,classification:public'
    },
    status: 'updated'
  },
  {
    id: '15',
    name: 'Payment Information',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Order Management'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Payment Information',
      description: 'Customer payment details and methods',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: '',
      parent_nodes: 'Order Management',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:high,classification:confidential'
    },
    status: 'conflict'
  },
  {
    id: '16',
    name: 'Analytics Terms',
    type: 'glossaryNode',
    urn: '',
    parentNames: [],
    parentUrns: [],
    level: 0,
    data: {
      entity_type: 'glossaryNode',
      urn: '',
      name: 'Analytics Terms',
      description: 'Data analytics and reporting terminology',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: '',
      parent_nodes: '',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'category:analytics,department:data'
    },
    status: 'existing'
  },
  {
    id: '17',
    name: 'Conversion Rate',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Analytics Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Conversion Rate',
      description: 'Percentage of visitors who complete a desired action',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Analytics Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:low,classification:public'
    },
    status: 'new'
  },
  {
    id: '18',
    name: 'Revenue',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Analytics Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Revenue',
      description: 'Total income generated from sales',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'datahub:Technical Owner',
      ownership_groups: '',
      parent_nodes: 'Analytics Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:medium,classification:internal'
    },
    status: 'updated'
  },
  {
    id: '19',
    name: 'Customer Lifetime Value',
    type: 'glossaryTerm',
    urn: '',
    parentNames: ['Analytics Terms'],
    parentUrns: [],
    level: 1,
    data: {
      entity_type: 'glossaryTerm',
      urn: '',
      name: 'Customer Lifetime Value',
      description: 'Predicted revenue from a customer over their lifetime',
      term_source: 'INTERNAL',
      source_ref: '',
      source_url: '',
      ownership_users: 'admin:DEVELOPER',
      ownership_groups: 'bfoo:Technical Owner',
      parent_nodes: 'Analytics Terms',
      related_contains: '',
      related_inherits: '',
      domain_urn: '',
      domain_name: '',
      custom_properties: 'sensitivity:medium,classification:internal'
    },
    status: 'conflict'
  }
];

// Mock comparison result
export const mockComparisonResult: ComparisonResult = {
  newEntities: [
    mockEntities[1], // Customer Name
    mockEntities[4], // Phone Number
    mockEntities[6], // Date of Birth
    mockEntities[8], // Product ID
    mockEntities[12], // Order ID
    mockEntities[16] // Conversion Rate
  ],
  existingEntities: [
    mockEntities[2], // Business Terms
    mockEntities[7], // Product Catalog
    mockEntities[11], // Order Management
    mockEntities[15] // Analytics Terms
  ],
  updatedEntities: [
    mockEntities[0], // Customer ID
    mockEntities[5], // Address
    mockEntities[9], // Product Name
    mockEntities[13], // Order Status
    mockEntities[17] // Revenue
  ],
  conflicts: [
    mockEntities[3], // Email Address
    mockEntities[10], // Product Price
    mockEntities[14], // Payment Information
    mockEntities[18] // Customer Lifetime Value
  ]
};

// Mock progress states
export const mockProgressStates = {
  idle: {
    total: 0,
    processed: 0,
    successful: 0,
    failed: 0,
    currentBatch: 0,
    totalBatches: 0,
    errors: []
  } as ImportProgress,
  
  processing: {
    total: 19,
    processed: 8,
    successful: 6,
    failed: 2,
    currentBatch: 2,
    totalBatches: 4,
    currentEntity: mockEntities[8], // Product ID
    errors: []
  } as ImportProgress,
  
  completed: {
    total: 19,
    processed: 19,
    successful: 19,
    failed: 0,
    currentBatch: 4,
    totalBatches: 4,
    errors: []
  } as ImportProgress,
  
  withErrors: {
    total: 19,
    processed: 15,
    successful: 13,
    failed: 2,
    currentBatch: 3,
    totalBatches: 4,
    errors: [
      {
        entity: mockEntities[3], // Email Address
        error: 'Validation failed: Invalid email format'
      },
      {
        entity: mockEntities[10], // Product Price
        error: 'Failed to create entity: URN already exists'
      }
    ]
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
