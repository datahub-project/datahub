// Entity URNs (using pre-seeded global data)
export const ENTITY_URNS = {
  DOMAIN: 'urn:li:domain:testing',
  DATA_PRODUCT: 'urn:li:dataProduct:testing',
  GLOSSARY_NODE: 'urn:li:glossaryNode:PlaywrightNode',
  GLOSSARY_TERM: 'urn:li:glossaryTerm:PlaywrightNode.PlaywrightTerm',
} as const;

// Property keys
export const PROPERTY_KEYS = {
  CREATED: 'CREATED',
  OWNERS: 'OWNERS',
  DOMAIN: 'DOMAIN',
  TAGS: 'TAGS',
  GLOSSARY_TERMS: 'GLOSSARY_TERMS',
} as const;

// Link test data
export const LINK_LABELS = {
  INITIAL: 'testLink',
  UPDATED: 'testLinkUpdated',
} as const;

// Description test data
export const DESCRIPTIONS = {
  INITIAL: 'Description test',
  UPDATED: 'Updated description',
} as const;

// URL base for link tests
export const LINK_URL_BASE = 'https://test.com';
export const LINK_URL_BASE_UPDATED = 'https://test-updated.com';

// Owner URN for validation
export const OWNER_URN = 'urn:li:corpuser:jdoe';

// Module types
export const MODULE_TYPES = {
  ASSETS: 'assets',
  HIERARCHY: 'hierarchy',
  DATA_PRODUCTS: 'data-products',
  RELATED_TERMS: 'related-terms',
} as const;

// Expected values in properties and modules
export const EXPECTED_VALUES = {
  // Data Product (with global pre-seeded data)
  DATA_PRODUCT_DOMAIN: 'testing',
  DATA_PRODUCT_TAG: 'Playwright',
  DATA_PRODUCT_GLOSSARY_TERM: 'PlaywrightTerm',
  DATA_PRODUCT_ASSETS_MODULE: 'Baz Dashboard',

  // Domain
  DOMAIN_ASSETS_MODULE: 'Baz Dashboard',
  DOMAIN_HIERARCHY_MODULE: 'Subdomain',
  DOMAIN_DATA_PRODUCTS_MODULE: 'Testing',

  // Glossary Node
  GLOSSARY_NODE_HIERARCHY_MODULE: 'PlaywrightTerm',

  // Glossary Term (with global pre-seeded data)
  GLOSSARY_TERM_DOMAIN: 'testing',
  GLOSSARY_TERM_ASSETS_MODULE: 'Testing',
  GLOSSARY_TERM_RELATED_TERMS_MODULE: 'RelatedPlaywrightTerm',
} as const;
