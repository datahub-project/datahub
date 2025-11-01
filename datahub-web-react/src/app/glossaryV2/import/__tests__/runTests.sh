#!/bin/bash

# Test script for CSV import functionality
# This script runs all tests related to the CSV import feature

echo "🧪 Running CSV Import Tests"
echo "=========================="

# Change to the datahub-web-react directory
cd "$(dirname "$0")/../../../../.."

# Run unit tests for ownership parsing utilities
echo ""
echo "📋 Running Unit Tests for Ownership Parsing Utilities..."
yarn test src/app/glossaryV2/import/shared/utils/__tests__/ownershipParsingUtils.test.ts --run

# Run unit tests for glossary utilities
echo ""
echo "📋 Running Unit Tests for Glossary Utilities..."
yarn test src/app/glossaryV2/import/shared/utils/__tests__/glossaryUtils.test.ts --run

# Run unit tests for custom properties utilities
echo ""
echo "📋 Running Unit Tests for Custom Properties Utilities..."
yarn test src/app/glossaryV2/import/shared/utils/__tests__/customPropertiesUtils.test.ts --run

# Run integration tests for useImportProcessing hook
echo ""
echo "🔗 Running Integration Tests for useImportProcessing Hook..."
yarn test src/app/glossaryV2/import/shared/hooks/__tests__/useImportProcessing.test.tsx --run

# Run end-to-end tests for CSV import workflow
echo ""
echo "🚀 Running End-to-End Tests for CSV Import Workflow..."
yarn test src/app/glossaryV2/import/__tests__/csvImportE2E.test.tsx --run

# Run complete import workflow tests
echo ""
echo "🚀 Running Complete Import Workflow Tests..."
yarn test src/app/glossaryV2/import/__tests__/completeImportWorkflow.test.tsx --run

# Run all tests with coverage
echo ""
echo "📊 Running All Tests with Coverage..."
yarn test src/app/glossaryV2/import/ --coverage --run

echo ""
echo "✅ All tests completed!"
echo ""
echo "Test Summary:"
echo "- Unit tests: Ownership parsing utilities"
echo "- Unit tests: Glossary utilities (hierarchy, comparison, normalization)"
echo "- Unit tests: Custom properties utilities"
echo "- Integration tests: useImportProcessing hook"
echo "- End-to-end tests: Complete CSV import workflow"
echo "- Complete workflow tests: All import features"
echo "- Coverage: All import-related functionality"
