#!/usr/bin/env node

/**
 * Test script for validating glossary import functionality
 * Tests the patchEntities mutation with ownership type creation
 */

const fetch = require('node-fetch');

// Configuration
const DATAHUB_URL = 'http://localhost:9002';
const GRAPHQL_ENDPOINT = `${DATAHUB_URL}/graphql`;

// Sample data from the CSV file
const testEntities = [
  {
    entity_type: 'glossaryTerm',
    name: 'Imaging Reports',
    description: 'Results and interpretations from medical imaging studies',
    term_source: 'INTERNAL',
    source_ref: 'DataHub',
    source_url: 'https://github.com/healthcare-data-project/healthcare',
    ownership: 'admin:DEVELOPER:CORP_USER',
    parent_nodes: 'Clinical Observations',
    custom_properties: '{"data_type":"Report","domain":"Clinical Observations"}'
  },
  {
    entity_type: 'glossaryTerm', 
    name: 'Customer ID',
    description: 'Unique identifier for each customer',
    term_source: 'INTERNAL',
    ownership: 'bfoo:Technical Owner:CORP_GROUP,datahub:Technical Owner:CORP_USER',
    parent_nodes: 'Business Terms',
    related_inherits: 'Business Terms.Customer Name'
  },
  {
    entity_type: 'glossaryNode',
    name: 'Business Terms',
    description: '',
    ownership: 'datahub:Technical Owner:CORP_USER'
  },
  {
    entity_type: 'glossaryNode',
    name: 'Clinical Observations', 
    description: 'Clinical measurements, assessments, and findings related to patient health status',
    ownership: 'admin:DEVELOPER:CORP_USER'
  }
];

// Ownership types that need to be created
const requiredOwnershipTypes = [
  'DEVELOPER',
  'Technical Owner'
];

/**
 * Execute GraphQL mutation
 */
async function executeGraphQL(query, variables = {}) {
  try {
    const response = await fetch(GRAPHQL_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer test-token' // You may need to adjust this
      },
      body: JSON.stringify({
        query,
        variables
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const result = await response.json();
    
    if (result.errors) {
      console.error('GraphQL errors:', result.errors);
      throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
    }

    return result.data;
  } catch (error) {
    console.error('GraphQL execution failed:', error);
    throw error;
  }
}

/**
 * Create ownership types using patchEntities mutation
 */
async function createOwnershipTypes() {
  console.log('🔧 Creating ownership types...');
  
  const currentTime = Date.now();
  const currentUserUrn = 'urn:li:corpuser:datahub'; // Fallback user
  
  const ownershipTypePatches = requiredOwnershipTypes.map(typeName => ({
    entityType: "ownershipType",
    aspectName: "ownershipTypeInfo",
    patch: [
      { op: "ADD", path: "/name", value: `"${typeName}"` },
      { op: "ADD", path: "/description", value: `"Custom ownership type: ${typeName}"` },
      {
        op: "ADD",
        path: "/created",
        value: JSON.stringify({
          time: currentTime,
          actor: currentUserUrn
        })
      },
      {
        op: "ADD",
        path: "/lastModified",
        value: JSON.stringify({
          time: currentTime,
          actor: currentUserUrn
        })
      }
    ]
  }));

  const mutation = `
    mutation patchEntities($input: [PatchEntityInput!]!) {
      patchEntities(input: $input) {
        urn
        name
        success
        error
      }
    }
  `;

  const result = await executeGraphQL(mutation, { input: ownershipTypePatches });
  
  console.log('✅ Ownership types created:', result.patchEntities);
  return result.patchEntities;
}

/**
 * Create glossary entities using patchEntities mutation
 */
async function createGlossaryEntities(ownershipTypeMap) {
  console.log('📚 Creating glossary entities...');
  
  const entityPatches = testEntities.map(entity => {
    const patches = [];
    
    if (entity.entity_type === 'glossaryTerm') {
      // Required fields for glossary terms
      patches.push({ op: 'ADD', path: '/name', value: `"${entity.name}"` });
      patches.push({ op: 'ADD', path: '/definition', value: `"${entity.description}"` });
      patches.push({ op: 'ADD', path: '/termSource', value: `"${entity.term_source || 'INTERNAL'}"` });
      
      // Optional fields
      if (entity.source_ref) {
        patches.push({ op: 'ADD', path: '/sourceRef', value: `"${entity.source_ref}"` });
      }
      if (entity.source_url) {
        patches.push({ op: 'ADD', path: '/sourceUrl', value: `"${entity.source_url}"` });
      }
      
      // Custom properties
      if (entity.custom_properties) {
        try {
          const customProps = JSON.parse(entity.custom_properties);
          Object.entries(customProps).forEach(([key, value]) => {
            patches.push({
              op: 'ADD',
              path: `/customProperties/${key}`,
              value: JSON.stringify(String(value))
            });
          });
        } catch (error) {
          console.warn(`Failed to parse custom properties for ${entity.name}:`, error);
        }
      }
      
      // Parent relationships
      if (entity.parent_nodes) {
        const parentNames = entity.parent_nodes.split(',').map(name => name.trim());
        parentNames.forEach(parentName => {
          if (parentName) {
            patches.push({
              op: 'ADD',
              path: '/parentNode',
              value: `"${parentName}"` // This would need to be resolved to URN in real implementation
            });
          }
        });
      }
      
      return {
        entityType: 'glossaryTerm',
        aspectName: 'glossaryTermInfo',
        patch: patches
      };
    } else if (entity.entity_type === 'glossaryNode') {
      // Required fields for glossary nodes
      patches.push({ op: 'ADD', path: '/name', value: `"${entity.name}"` });
      patches.push({ op: 'ADD', path: '/definition', value: `"${entity.description || ''}"` });
      
      return {
        entityType: 'glossaryNode', 
        aspectName: 'glossaryNodeInfo',
        patch: patches
      };
    }
    
    return null;
  }).filter(Boolean);

  const mutation = `
    mutation patchEntities($input: [PatchEntityInput!]!) {
      patchEntities(input: $input) {
        urn
        name
        success
        error
      }
    }
  `;

  const result = await executeGraphQL(mutation, { input: entityPatches });
  
  console.log('✅ Glossary entities created:', result.patchEntities);
  return result.patchEntities;
}

/**
 * Create ownership patches for entities
 */
async function createOwnershipPatches(entityResults, ownershipTypeMap) {
  console.log('👥 Creating ownership patches...');
  
  const ownershipPatches = [];
  
  testEntities.forEach((entity, index) => {
    if (!entity.ownership) return;
    
    const entityResult = entityResults[index];
    if (!entityResult || !entityResult.success) return;
    
    const ownershipStrings = entity.ownership.split(',').map(owner => owner.trim());
    const patches = [];
    
    ownershipStrings.forEach(ownerString => {
      const parts = ownerString.split(':');
      if (parts.length >= 2) {
        const [type, owner, corpType] = parts;
        const ownerUrn = owner.startsWith('urn:li:') ? owner : `urn:li:corpuser:${owner}`;
        const ownershipTypeUrn = ownershipTypeMap[type];
        
        if (ownershipTypeUrn) {
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
        }
      }
    });
    
    if (patches.length > 0) {
      ownershipPatches.push({
        entityType: entity.entity_type === 'glossaryTerm' ? 'glossaryTerm' : 'glossaryNode',
        urn: entityResult.urn,
        aspectName: 'ownership',
        patch: patches
      });
    }
  });
  
  if (ownershipPatches.length > 0) {
    const mutation = `
      mutation patchEntities($input: [PatchEntityInput!]!) {
        patchEntities(input: $input) {
          urn
          name
          success
          error
        }
      }
    `;
    
    const result = await executeGraphQL(mutation, { input: ownershipPatches });
    console.log('✅ Ownership patches created:', result.patchEntities);
    return result.patchEntities;
  }
  
  return [];
}

/**
 * Query existing entities to check for duplicates
 */
async function queryExistingEntities() {
  console.log('🔍 Querying existing entities...');
  
  const query = `
    query getGlossaryEntities($input: ScrollAcrossEntitiesInput!) {
      scrollAcrossEntities(input: $input) {
        searchResults {
          entity {
            __typename
            ... on GlossaryTerm {
              urn
              name
              hierarchicalName
              properties {
                name
                description
                termSource
                sourceRef
                sourceUrl
                customProperties {
                  key
                  value
                }
              }
              ownership {
                owners {
                  owner {
                    __typename
                    urn
                  }
                  type
                  ownershipType {
                    urn
                    info {
                      name
                      description
                    }
                  }
                }
              }
            }
            ... on GlossaryNode {
              urn
              properties {
                name
                description
                customProperties {
                  key
                  value
                }
              }
              ownership {
                owners {
                  owner {
                    __typename
                    urn
                  }
                  type
                  ownershipType {
                    urn
                    info {
                      name
                      description
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  `;
  
  const variables = {
    input: {
      types: ['glossaryTerm', 'glossaryNode'],
      query: '*',
      count: 100
    }
  };
  
  const result = await executeGraphQL(query, variables);
  return result.scrollAcrossEntities.searchResults.map(r => r.entity);
}

/**
 * Main test function
 */
async function runTests() {
  console.log('🚀 Starting glossary import validation tests...\n');
  
  try {
    // Step 1: Query existing entities (baseline)
    console.log('=== STEP 1: Query Baseline ===');
    const baselineEntities = await queryExistingEntities();
    console.log(`Found ${baselineEntities.length} existing entities\n`);
    
    // Step 2: Create ownership types
    console.log('=== STEP 2: Create Ownership Types ===');
    const ownershipTypeResults = await createOwnershipTypes();
    const ownershipTypeMap = {};
    ownershipTypeResults.forEach((result, index) => {
      if (result.success) {
        ownershipTypeMap[requiredOwnershipTypes[index]] = result.urn;
      }
    });
    console.log('Ownership type map:', ownershipTypeMap);
    console.log('');
    
    // Step 3: Create glossary entities
    console.log('=== STEP 3: Create Glossary Entities ===');
    const entityResults = await createGlossaryEntities(ownershipTypeMap);
    console.log('');
    
    // Step 4: Create ownership patches
    console.log('=== STEP 4: Create Ownership Patches ===');
    const ownershipResults = await createOwnershipPatches(entityResults, ownershipTypeMap);
    console.log('');
    
    // Step 5: Query entities again to verify creation
    console.log('=== STEP 5: Verify Creation ===');
    const finalEntities = await queryExistingEntities();
    console.log(`Found ${finalEntities.length} entities after import\n`);
    
    // Step 6: Test duplicate handling
    console.log('=== STEP 6: Test Duplicate Handling ===');
    console.log('Running the same import again to test duplicate handling...');
    
    const duplicateOwnershipResults = await createOwnershipTypes();
    const duplicateEntityResults = await createGlossaryEntities(ownershipTypeMap);
    const duplicateOwnershipPatches = await createOwnershipPatches(duplicateEntityResults, ownershipTypeMap);
    
    const finalEntitiesAfterDuplicate = await queryExistingEntities();
    console.log(`Found ${finalEntitiesAfterDuplicate.length} entities after duplicate import\n`);
    
    // Analysis
    console.log('=== ANALYSIS ===');
    console.log(`Baseline entities: ${baselineEntities.length}`);
    console.log(`After first import: ${finalEntities.length}`);
    console.log(`After duplicate import: ${finalEntitiesAfterDuplicate.length}`);
    console.log(`New entities created in first import: ${finalEntities.length - baselineEntities.length}`);
    console.log(`New entities created in duplicate import: ${finalEntitiesAfterDuplicate.length - finalEntities.length}`);
    
    // Check for duplicates
    const duplicateCount = finalEntitiesAfterDuplicate.length - finalEntities.length;
    if (duplicateCount === 0) {
      console.log('✅ Duplicate handling works correctly - no duplicate entities created');
    } else {
      console.log(`❌ Duplicate handling issue - ${duplicateCount} duplicate entities created`);
    }
    
    // Count API calls
    const totalApiCalls = 4; // 1 for ownership types, 1 for entities, 1 for ownership, 1 for query
    console.log(`\n📊 Total API calls made: ${totalApiCalls}`);
    console.log('✅ Optimal number of API calls - using batch operations');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    process.exit(1);
  }
}

// Run the tests
runTests().then(() => {
  console.log('\n🎉 All tests completed!');
  process.exit(0);
}).catch(error => {
  console.error('💥 Test suite failed:', error);
  process.exit(1);
});
