#!/usr/bin/env node

/**
 * Script to switch between real and mock CSV import functionality
 * Usage: node scripts/switch-to-mock.js [real|mock]
 */

const fs = require('fs');
const path = require('path');

const mode = process.argv[2] || 'mock';
const basePath = path.join(__dirname, '..');

// Files to modify
const filesToModify = [
  'WizardPage/WizardPage.tsx',
  'WizardPage/WizardPage.hooks.ts'
];

// Mock replacements
const mockReplacements = {
  'useImportProcessing': 'useMockImportProcessing',
  'useCsvProcessing': 'useMockCsvProcessing', 
  'useEntityManagement': 'useMockEntityManagement',
  'useEntityComparison': 'useMockEntityComparison',
  'useGraphQLOperations': 'useMockGraphQLOperations',
  'useFileUpload': 'useMockFileUpload',
  'useFileValidation': 'useMockFileValidation'
};

// Real replacements (reverse of mock)
const realReplacements = Object.fromEntries(
  Object.entries(mockReplacements).map(([key, value]) => [value, key])
);

function switchToMock() {
  console.log('🔄 Switching to MOCK mode...');
  
  filesToModify.forEach(file => {
    const filePath = path.join(basePath, file);
    
    if (fs.existsSync(filePath)) {
      let content = fs.readFileSync(filePath, 'utf8');
      
      // Apply mock replacements
      Object.entries(mockReplacements).forEach(([real, mock]) => {
        const regex = new RegExp(`\\b${real}\\b`, 'g');
        content = content.replace(regex, mock);
      });
      
      // Update imports
      content = content.replace(
        /from '\.\.\/shared\/hooks\/(useImportProcessing|useCsvProcessing|useEntityManagement|useEntityComparison|useGraphQLOperations)'/g,
        (match, hook) => `from '../shared/hooks/useMock${hook.slice(2)}'`
      );
      
      content = content.replace(
        /from '\.\/DropzoneTable\/DropzoneTable\.hooks'/g,
        "from './DropzoneTable/useMockFileUpload'"
      );
      
      fs.writeFileSync(filePath, content);
      console.log(`✅ Updated ${file}`);
    } else {
      console.log(`⚠️  File not found: ${file}`);
    }
  });
  
  console.log('🎉 Switched to MOCK mode!');
  console.log('📝 Access the mock UI at: http://localhost:3000/glossaryV2/import/mock');
}

function switchToReal() {
  console.log('🔄 Switching to REAL mode...');
  
  filesToModify.forEach(file => {
    const filePath = path.join(basePath, file);
    
    if (fs.existsSync(filePath)) {
      let content = fs.readFileSync(filePath, 'utf8');
      
      // Apply real replacements
      Object.entries(realReplacements).forEach(([mock, real]) => {
        const regex = new RegExp(`\\b${mock}\\b`, 'g');
        content = content.replace(regex, real);
      });
      
      // Update imports back to real
      content = content.replace(
        /from '\.\.\/shared\/hooks\/useMock(ImportProcessing|CsvProcessing|EntityManagement|EntityComparison|GraphQLOperations)'/g,
        (match, hook) => `from '../shared/hooks/use${hook}'`
      );
      
      content = content.replace(
        /from '\.\/DropzoneTable\/useMockFileUpload'/g,
        "from './DropzoneTable/DropzoneTable.hooks'"
      );
      
      fs.writeFileSync(filePath, content);
      console.log(`✅ Updated ${file}`);
    } else {
      console.log(`⚠️  File not found: ${file}`);
    }
  });
  
  console.log('🎉 Switched to REAL mode!');
  console.log('📝 Access the real UI at: http://localhost:3000/glossaryV2/import');
}

// Main execution
if (mode === 'mock') {
  switchToMock();
} else if (mode === 'real') {
  switchToReal();
} else {
  console.log('❌ Invalid mode. Use "real" or "mock"');
  console.log('Usage: node scripts/switch-to-mock.js [real|mock]');
  process.exit(1);
}
