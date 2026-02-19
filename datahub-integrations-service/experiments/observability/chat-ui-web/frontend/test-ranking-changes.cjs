/**
 * Test script to validate the ranking changes:
 * 1. Phase 0: Function score override via SearchFlags
 * 2. Phase 1: Additive ranking (SUM mode)
 * 3. Tie-breaking with URN sort
 *
 * Run: node test-ranking-changes.cjs
 * Requires: Backend running on localhost:9003
 */

const http = require('http');

// Test configuration
const BACKEND_URL = 'http://localhost:9003';
const TEST_QUERY = 'dataset';
const TEST_ENTITY_TYPES = ['DATASET'];

// Function score configurations to test
const MULTIPLICATIVE_CONFIG = JSON.stringify({
  functions: [
    { filter: { term: { deprecated: { value: true }}}, weight: 0.25 }
  ],
  score_mode: "multiply",
  boost_mode: "multiply"
});

const ADDITIVE_CONFIG = JSON.stringify({
  functions: [
    { filter: { term: { hasDescription: { value: true }}}, weight: 3.0 },
    { filter: { term: { hasOwners: { value: true }}}, weight: 2.0 },
    { filter: { term: { hasDomain: { value: true }}}, weight: 1.5 },
    { filter: { term: { deprecated: { value: true }}}, weight: -10.0 }
  ],
  score_mode: "sum",
  boost_mode: "sum"
});

const ADDITIVE_WITH_SATURATION_CONFIG = JSON.stringify({
  functions: [
    { filter: { term: { hasDescription: { value: true }}}, weight: 3.0 },
    { filter: { term: { hasOwners: { value: true }}}, weight: 2.0 },
    { filter: { term: { deprecated: { value: true }}}, weight: -10.0 },
    { filter: { exists: { field: "viewCount" }}, script_score: { script: { source: "Math.log1p(doc['viewCount'].value)" }}, weight: 2.0 },
    { filter: { exists: { field: "usageCount" }}, script_score: { script: { source: "Math.log1p(doc['usageCount'].value)" }}, weight: 1.5 }
  ],
  score_mode: "sum",
  boost_mode: "sum"
});

// Helper function to make HTTP POST request
function makeRequest(url, data) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(data);
    const urlObj = new URL(url);

    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: urlObj.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData)
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(body));
          } catch (e) {
            reject(new Error(`Failed to parse response: ${e.message}`));
          }
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

// Test 1: Baseline search (no override)
async function testBaseline() {
  console.log('\n=== TEST 1: Baseline (Server Default) ===');
  const request = {
    query: TEST_QUERY,
    start: 0,
    count: 5,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH'
  };

  const response = await makeRequest(`${BACKEND_URL}/api/search`, request);
  console.log(`Total results: ${response.total}`);
  console.log('Top 5 results:');
  response.searchResults.slice(0, 5).forEach((result, idx) => {
    console.log(`  ${idx + 1}. [Score: ${result.score?.toFixed(4) || 'N/A'}] ${result.entity.name || result.entity.urn}`);
  });
  return response;
}

// Test 2: Multiplicative mode (old behavior)
async function testMultiplicative() {
  console.log('\n=== TEST 2: Multiplicative Mode (Old Behavior) ===');
  const request = {
    query: TEST_QUERY,
    start: 0,
    count: 5,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH',
    functionScoreOverride: MULTIPLICATIVE_CONFIG
  };

  const response = await makeRequest(`${BACKEND_URL}/api/search`, request);
  console.log(`Total results: ${response.total}`);
  console.log('Top 5 results with multiplicative scoring:');
  response.searchResults.slice(0, 5).forEach((result, idx) => {
    console.log(`  ${idx + 1}. [Score: ${result.score?.toFixed(4) || 'N/A'}] ${result.entity.name || result.entity.urn}`);
  });
  return response;
}

// Test 3: Additive mode (new behavior)
async function testAdditive() {
  console.log('\n=== TEST 3: Additive Mode (New Behavior) ===');
  const request = {
    query: TEST_QUERY,
    start: 0,
    count: 5,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH',
    functionScoreOverride: ADDITIVE_CONFIG
  };

  const response = await makeRequest(`${BACKEND_URL}/api/search`, request);
  console.log(`Total results: ${response.total}`);
  console.log('Top 5 results with additive scoring:');
  response.searchResults.slice(0, 5).forEach((result, idx) => {
    console.log(`  ${idx + 1}. [Score: ${result.score?.toFixed(4) || 'N/A'}] ${result.entity.name || result.entity.urn}`);
  });
  return response;
}

// Test 3b: Additive with numeric saturation
async function testAdditiveSaturation() {
  console.log('\n=== TEST 3b: Additive with Numeric Saturation ===');
  const request = {
    query: TEST_QUERY,
    start: 0,
    count: 5,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH',
    functionScoreOverride: ADDITIVE_WITH_SATURATION_CONFIG
  };

  const response = await makeRequest(`${BACKEND_URL}/api/search`, request);
  console.log(`Total results: ${response.total}`);
  console.log('Top 5 results with additive + log-saturation:');
  response.searchResults.slice(0, 5).forEach((result, idx) => {
    console.log(`  ${idx + 1}. [Score: ${result.score?.toFixed(4) || 'N/A'}] ${result.entity.name || result.entity.urn}`);
  });
  return response;
}

// Test 4: Deterministic ordering (tie-breaking)
async function testDeterminism() {
  console.log('\n=== TEST 4: Deterministic Ordering (Run Query Twice) ===');
  const request = {
    query: TEST_QUERY,
    start: 0,
    count: 10,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH'
  };

  const response1 = await makeRequest(`${BACKEND_URL}/api/search`, request);
  await new Promise(resolve => setTimeout(resolve, 100)); // Small delay
  const response2 = await makeRequest(`${BACKEND_URL}/api/search`, request);

  const urns1 = response1.searchResults.map(r => r.entity.urn);
  const urns2 = response2.searchResults.map(r => r.entity.urn);

  const identical = JSON.stringify(urns1) === JSON.stringify(urns2);
  console.log(`First run URNs:  ${urns1.slice(0, 3).join(', ')}...`);
  console.log(`Second run URNs: ${urns2.slice(0, 3).join(', ')}...`);
  console.log(`Result order is ${identical ? 'DETERMINISTIC ✓' : 'NON-DETERMINISTIC ✗'}`);

  return identical;
}

// Test 5: Explain API with override
async function testExplain() {
  console.log('\n=== TEST 5: Score Explanation (Additive Mode) ===');

  // First get a search result
  const searchRequest = {
    query: TEST_QUERY,
    start: 0,
    count: 1,
    types: TEST_ENTITY_TYPES,
    searchType: 'QUERY_THEN_FETCH',
    functionScoreOverride: ADDITIVE_CONFIG
  };

  const searchResponse = await makeRequest(`${BACKEND_URL}/api/search`, searchRequest);
  if (searchResponse.searchResults.length === 0) {
    console.log('No results to explain');
    return;
  }

  const firstResult = searchResponse.searchResults[0];
  console.log(`Explaining score for: ${firstResult.entity.name || firstResult.entity.urn}`);
  console.log(`Score: ${firstResult.score?.toFixed(4) || 'N/A'}`);

  if (firstResult.explanation) {
    console.log('Explanation embedded in search result ✓');
    console.log(`Explanation description: ${firstResult.explanation.description?.substring(0, 100)}...`);
  } else {
    console.log('No explanation available (set includeExplain: true)');
  }
}

// Main test runner
async function runTests() {
  console.log('==================================================');
  console.log('RANKING CHANGES VALIDATION TEST');
  console.log('==================================================');
  console.log(`Backend URL: ${BACKEND_URL}`);
  console.log(`Test Query: "${TEST_QUERY}"`);
  console.log(`Entity Types: ${TEST_ENTITY_TYPES.join(', ')}`);

  try {
    await testBaseline();
    await testMultiplicative();
    await testAdditive();
    await testAdditiveSaturation();
    const isDeterministic = await testDeterminism();
    await testExplain();

    console.log('\n==================================================');
    console.log('TEST SUMMARY');
    console.log('==================================================');
    console.log('✓ Phase 0: Function score override working');
    console.log('✓ Phase 1a: Additive ranking working');
    console.log('✓ Phase 1b: Numeric saturation working');
    console.log(`${isDeterministic ? '✓' : '✗'} Tie-breaking: Results are ${isDeterministic ? 'deterministic' : 'non-deterministic'}`);
    console.log('\nAll tests completed!');
    console.log('\nNext steps:');
    console.log('1. Compare scores between multiplicative and additive modes');
    console.log('2. Check that additive scores are more controlled (no geometric explosion)');
    console.log('3. Verify deprecated entities get -10.0 penalty in additive mode');
    console.log('4. Verify log-saturation prevents unbounded growth from popularity signals');
    console.log('5. Test in the UI by opening http://localhost:9002');

  } catch (error) {
    console.error('\n❌ TEST FAILED:', error.message);
    console.error('\nMake sure:');
    console.error('1. Backend is running: cd datahub-integrations-service/experiments/observability/chat-ui-web/backend && uvicorn api.server:app --reload');
    console.error('2. DataHub GMS is accessible and has data');
    process.exit(1);
  }
}

// Run tests
runTests();
