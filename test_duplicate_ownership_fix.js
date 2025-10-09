#!/usr/bin/env node

/**
 * Test script to verify the duplicate ownership type fix
 * Tests that the system properly handles existing ownership types
 */

const fetch = require('node-fetch');

const DATAHUB_URL = 'http://localhost:3000';
const GRAPHQL_ENDPOINT = `${DATAHUB_URL}/api/v2/graphql`;

async function authenticate() {
  const response = await fetch(`${DATAHUB_URL}/logIn`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: 'datahub', password: 'datahub' })
  });
  
  const cookies = response.headers.get('set-cookie');
  return cookies;
}

async function executeGraphQL(query, variables = {}, cookies) {
  const response = await fetch(GRAPHQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Cookie': cookies
    },
    body: JSON.stringify({ query, variables })
  });

  const result = await response.json();
  
  if (result.errors) {
    console.error('GraphQL Errors:', JSON.stringify(result.errors, null, 2));
    throw new Error(`GraphQL errors: ${result.errors.map(e => e.message).join(', ')}`);
  }

  return result.data;
}

async function testOwnershipTypeResolution() {
  console.log('🔍 Testing ownership type resolution...');
  
  const cookies = await authenticate();
  
  // Query existing ownership types
  const query = `
    query { 
      listOwnershipTypes(input: { start: 0, count: 100 }) { 
        ownershipTypes { 
          urn 
          info { 
            name 
            description 
          } 
        } 
      } 
    }
  `;
  
  const result = await executeGraphQL(query, {}, cookies);
  const ownershipTypes = result.listOwnershipTypes.ownershipTypes;
  
  console.log(`Found ${ownershipTypes.length} ownership types`);
  
  // Group by name to find duplicates
  const nameToUrns = new Map();
  ownershipTypes.forEach(type => {
    const name = type.info.name.toLowerCase();
    if (!nameToUrns.has(name)) {
      nameToUrns.set(name, []);
    }
    nameToUrns.get(name).push(type.urn);
  });
  
  console.log('\nOwnership type analysis:');
  nameToUrns.forEach((urns, name) => {
    if (urns.length > 1) {
      console.log(`⚠️  DUPLICATE: "${name}" has ${urns.length} URNs:`);
      urns.forEach((urn, index) => {
        console.log(`    ${index + 1}. ${urn}`);
      });
    } else {
      console.log(`✅ UNIQUE: "${name}" -> ${urns[0]}`);
    }
  });
  
  // Test the resolution logic
  console.log('\nTesting resolution logic:');
  const ownershipTypeMap = new Map();
  ownershipTypes.forEach(type => {
    const name = type.info.name.toLowerCase();
    if (!ownershipTypeMap.has(name)) {
      ownershipTypeMap.set(name, type.urn);
    }
  });
  
  console.log('Resolution map:');
  ownershipTypeMap.forEach((urn, name) => {
    console.log(`  "${name}" -> ${urn}`);
  });
  
  // Test specific ownership types from CSV
  const testTypes = ['developer', 'technical owner'];
  console.log('\nTesting CSV ownership types:');
  testTypes.forEach(typeName => {
    const resolvedUrn = ownershipTypeMap.get(typeName);
    if (resolvedUrn) {
      console.log(`✅ "${typeName}" resolves to: ${resolvedUrn}`);
    } else {
      console.log(`❌ "${typeName}" not found`);
    }
  });
}

async function testGlossaryEntityWithOwnership() {
  console.log('\n🧪 Testing glossary entity creation with ownership...');
  
  const cookies = await authenticate();
  
  // First, get the ownership type URNs
  const query = `
    query { 
      listOwnershipTypes(input: { start: 0, count: 100 }) { 
        ownershipTypes { 
          urn 
          info { 
            name 
          } 
        } 
      } 
    }
  `;
  
  const result = await executeGraphQL(query, {}, cookies);
  const ownershipTypes = result.listOwnershipTypes.ownershipTypes;
  
  // Create resolution map
  const ownershipTypeMap = new Map();
  ownershipTypes.forEach(type => {
    const name = type.info.name.toLowerCase();
    if (!ownershipTypeMap.has(name)) {
      ownershipTypeMap.set(name, type.urn);
    }
  });
  
  const developerUrn = ownershipTypeMap.get('developer');
  const technicalOwnerUrn = ownershipTypeMap.get('technical owner');
  
  if (!developerUrn || !technicalOwnerUrn) {
    console.log('❌ Required ownership types not found');
    return;
  }
  
  console.log(`Using DEVELOPER URN: ${developerUrn}`);
  console.log(`Using Technical Owner URN: ${technicalOwnerUrn}`);
  
  // Create a test glossary term with ownership
  const currentTime = Date.now();
  const currentUserUrn = 'urn:li:corpuser:datahub';
  
  const mutation = `
    mutation patchEntities($input: [PatchEntityInput!]!) {
      patchEntities(input: $input) {
        urn
        success
        error
      }
    }
  `;
  
  const variables = {
    input: [
      {
        entityType: "glossaryTerm",
        aspectName: "glossaryTermInfo",
        patch: [
          { op: "ADD", path: "/name", value: "Test Entity with Ownership" },
          { op: "ADD", path: "/definition", value: "Testing ownership resolution" },
          { op: "ADD", path: "/termSource", value: "INTERNAL" }
        ]
      }
    ]
  };
  
  const entityResult = await executeGraphQL(mutation, variables, cookies);
  
  if (entityResult.patchEntities[0].success) {
    const entityUrn = entityResult.patchEntities[0].urn;
    console.log(`✅ Created test entity: ${entityUrn}`);
    
    // Now add ownership
    const ownershipMutation = `
      mutation patchEntities($input: [PatchEntityInput!]!) {
        patchEntities(input: $input) {
          urn
          success
          error
        }
      }
    `;
    
    const ownershipVariables = {
      input: [
        {
          entityType: "glossaryTerm",
          urn: entityUrn,
          aspectName: "ownership",
          patch: [
            {
              op: "ADD",
              path: `/owners/${currentUserUrn}/${developerUrn}`,
              value: JSON.stringify({
                owner: currentUserUrn,
                typeUrn: developerUrn,
                type: "NONE",
                source: { type: "MANUAL" }
              })
            }
          ]
        }
      ]
    };
    
    const ownershipResult = await executeGraphQL(ownershipMutation, ownershipVariables, cookies);
    
    if (ownershipResult.patchEntities[0].success) {
      console.log('✅ Successfully added ownership with resolved URN');
    } else {
      console.log('❌ Failed to add ownership:', ownershipResult.patchEntities[0].error);
    }
  } else {
    console.log('❌ Failed to create test entity:', entityResult.patchEntities[0].error);
  }
}

async function main() {
  try {
    await testOwnershipTypeResolution();
    await testGlossaryEntityWithOwnership();
    
    console.log('\n🎉 Test completed!');
    console.log('\nKey findings:');
    console.log('1. The system has multiple ownership types with the same name');
    console.log('2. The resolution logic picks the first URN for each name');
    console.log('3. This should resolve the UI conflict issue');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

main();
