#!/usr/bin/env node

/**
 * Test script to validate multiple ownership parsing and handling
 */

// Simulate the enhanced ownership parsing logic
function parseMultipleOwnership(ownershipStr) {
  // Support both comma and pipe separators for multiple owners
  const ownershipStrings = ownershipStr
    .split(/[|,]/)
    .map(owner => owner.trim())
    .filter(owner => owner.length > 0);
  
  const results = [];
  
  ownershipStrings.forEach((ownerString) => {
    // Parse ownership string format: "ownershipType:owner:corpType"
    const parts = ownerString.split(':');
    if (parts.length >= 2) {
      const [ownershipTypeName, ownerName, corpType] = parts;
      
      // Format owner URN based on corpType
      let ownerUrn;
      if (ownerName.startsWith('urn:li:')) {
        ownerUrn = ownerName;
      } else if (corpType === 'CORP_GROUP') {
        ownerUrn = `urn:li:corpGroup:${ownerName}`;
      } else {
        ownerUrn = `urn:li:corpuser:${ownerName}`;
      }
      
      // Determine the owner type based on ownership type name
      let ownerType = 'NONE';
      if (ownershipTypeName.toLowerCase().includes('technical')) {
        ownerType = 'TECHNICAL_OWNER';
      } else if (ownershipTypeName.toLowerCase().includes('business')) {
        ownerType = 'BUSINESS_OWNER';
      } else if (ownershipTypeName.toLowerCase().includes('data')) {
        ownerType = 'DATA_STEWARD';
      }
      
      results.push({
        owner: ownerUrn,
        ownershipTypeName: ownershipTypeName,
        ownerType: ownerType,
        corpType: corpType || 'CORP_USER',
        originalString: ownerString
      });
    }
  });
  
  return results;
}

// Test cases for multiple ownership
const testCases = [
  {
    input: "admin:DEVELOPER:CORP_USER",
    description: "Single ownership (comma format)",
    expectedCount: 1
  },
  {
    input: "bfoo:Technical Owner:CORP_GROUP,datahub:Technical Owner:CORP_USER",
    description: "Multiple ownership (comma separator)",
    expectedCount: 2
  },
  {
    input: "admin:DEVELOPER:CORP_USER|bfoo:Technical Owner:CORP_GROUP|datahub:Technical Owner:CORP_USER",
    description: "Multiple ownership (pipe separator)",
    expectedCount: 3
  },
  {
    input: "admin:DEVELOPER:CORP_USER|bfoo:Technical Owner:CORP_GROUP|datahub:Technical Owner:CORP_USER|business:Product Owner:CORP_USER",
    description: "Many owners (pipe separator)",
    expectedCount: 4
  },
  {
    input: "admin:DEVELOPER:CORP_USER,datahub:Technical Owner:CORP_USER|business:Product Owner:CORP_USER",
    description: "Mixed separators (comma and pipe)",
    expectedCount: 3
  },
  {
    input: "admin:DEVELOPER:CORP_USER||datahub:Technical Owner:CORP_USER",
    description: "Empty entries (should be filtered)",
    expectedCount: 2
  }
];

console.log("🧪 Testing Multiple Ownership Parsing\n");

testCases.forEach((testCase, index) => {
  console.log(`=== Test Case ${index + 1}: ${testCase.description} ===`);
  console.log(`Input: "${testCase.input}"`);
  
  const result = parseMultipleOwnership(testCase.input);
  
  console.log(`Expected: ${testCase.expectedCount} owners`);
  console.log(`Actual: ${result.length} owners`);
  
  if (result.length === testCase.expectedCount) {
    console.log("✅ PASS");
  } else {
    console.log("❌ FAIL");
  }
  
  console.log("Parsed owners:");
  result.forEach((ownership, i) => {
    console.log(`  ${i + 1}. ${ownership.originalString}`);
    console.log(`     → Owner: ${ownership.owner}`);
    console.log(`     → Type: ${ownership.ownershipTypeName} (${ownership.ownerType})`);
    console.log(`     → Corp: ${ownership.corpType}`);
  });
  
  console.log("");
});

// Test edge cases
console.log("=== Edge Cases ===");

const edgeCases = [
  {
    input: "",
    description: "Empty string"
  },
  {
    input: "   ",
    description: "Whitespace only"
  },
  {
    input: "invalid:format",
    description: "Invalid format (missing corpType)"
  },
  {
    input: "admin:DEVELOPER:CORP_USER,invalid:format,datahub:Technical Owner:CORP_USER",
    description: "Mixed valid and invalid entries"
  }
];

edgeCases.forEach((testCase, index) => {
  console.log(`\nEdge Case ${index + 1}: ${testCase.description}`);
  console.log(`Input: "${testCase.input}"`);
  
  const result = parseMultipleOwnership(testCase.input);
  console.log(`Result: ${result.length} valid owners parsed`);
  
  if (result.length > 0) {
    result.forEach((ownership, i) => {
      console.log(`  ${i + 1}. ${ownership.originalString} → ${ownership.owner}`);
    });
  }
});

console.log("\n=== CSV Format Recommendations ===");
console.log("✅ RECOMMENDED: Use pipe separator for multiple owners");
console.log("   Format: \"owner1|owner2|owner3\"");
console.log("   Example: \"admin:DEVELOPER:CORP_USER|bfoo:Technical Owner:CORP_GROUP\"");
console.log("");
console.log("✅ ALTERNATIVE: Use comma separator (current format)");
console.log("   Format: \"owner1,owner2,owner3\"");
console.log("   Example: \"admin:DEVELOPER:CORP_USER,bfoo:Technical Owner:CORP_GROUP\"");
console.log("");
console.log("⚠️  NOTE: Pipe separator is recommended to avoid conflicts with names containing commas");

console.log("\n🎉 Multiple ownership parsing test completed!");
