#!/usr/bin/env node

/**
 * Sync DataHub Styles Script
 * 
 * This script automatically extracts design tokens from the DataHub web-react
 * codebase and updates the tutorial component styles to match the actual UI.
 * 
 * Usage: node scripts/sync-datahub-styles.js
 */

const fs = require('fs');
const path = require('path');

// Paths
const DATAHUB_COLORS_PATH = '../../datahub-web-react/src/alchemy-components/theme/foundations/colors.ts';
const DATAHUB_SEMANTIC_TOKENS_PATH = '../../datahub-web-react/src/alchemy-components/theme/semantic-tokens.ts';
const DOCS_COMPONENTS_DIR = './src/components';

/**
 * Extract color values from DataHub's colors.ts file
 */
function extractDataHubColors() {
  try {
    const colorsFile = fs.readFileSync(path.resolve(__dirname, DATAHUB_COLORS_PATH), 'utf8');
    
    // Extract color definitions using regex
    const colorMatches = colorsFile.match(/(\w+):\s*{([^}]+)}/g) || [];
    const singleColorMatches = colorsFile.match(/(\w+):\s*'([^']+)'/g) || [];
    
    const colors = {};
    
    // Parse nested color objects (e.g., gray: { 100: '#EBECF0', ... })
    colorMatches.forEach(match => {
      const [, colorName, colorValues] = match.match(/(\w+):\s*{([^}]+)}/);
      const values = {};
      
      const valueMatches = colorValues.match(/(\d+):\s*'([^']+)'/g) || [];
      valueMatches.forEach(valueMatch => {
        const [, key, value] = valueMatch.match(/(\d+):\s*'([^']+)'/);
        values[key] = value;
      });
      
      colors[colorName] = values;
    });
    
    // Parse single color values (e.g., white: '#FFFFFF')
    singleColorMatches.forEach(match => {
      const [, colorName, colorValue] = match.match(/(\w+):\s*'([^']+)'/);
      colors[colorName] = colorValue;
    });
    
    return colors;
  } catch (error) {
    console.warn('Could not read DataHub colors file:', error.message);
    return null;
  }
}

/**
 * Extract semantic tokens from DataHub's semantic-tokens.ts file
 */
function extractSemanticTokens() {
  try {
    const semanticFile = fs.readFileSync(path.resolve(__dirname, DATAHUB_SEMANTIC_TOKENS_PATH), 'utf8');
    
    // Extract semantic token mappings
    const tokenMatches = semanticFile.match(/'([^']+)':\s*colors\.([^,\s]+)/g) || [];
    const tokens = {};
    
    tokenMatches.forEach(match => {
      const [, tokenName, colorPath] = match.match(/'([^']+)':\s*colors\.([^,\s]+)/);
      tokens[tokenName] = colorPath;
    });
    
    return tokens;
  } catch (error) {
    console.warn('Could not read DataHub semantic tokens file:', error.message);
    return null;
  }
}

/**
 * Generate CSS variables from DataHub colors
 */
function generateCSSVariables(colors, semanticTokens) {
  if (!colors) return '';
  
  let cssVars = `/* Auto-generated DataHub Design Tokens */\n:root {\n`;
  
  // Core color mappings based on DataHub's actual usage
  const colorMappings = {
    'datahub-primary': colors.primary?.[500] || colors.violet?.[500] || '#533FD1',
    'datahub-primary-dark': colors.primary?.[600] || colors.violet?.[600] || '#4C39BE',
    'datahub-primary-light': colors.primary?.[400] || colors.violet?.[400] || '#7565DA',
    'datahub-primary-lightest': colors.primary?.[0] || colors.violet?.[0] || '#F1F3FD',
    'datahub-gray-100': colors.gray?.[100] || '#EBECF0',
    'datahub-gray-600': colors.gray?.[600] || '#374066',
    'datahub-gray-1700': colors.gray?.[1700] || '#5F6685',
    'datahub-gray-1800': colors.gray?.[1800] || '#8088A3',
    'datahub-gray-1500': colors.gray?.[1500] || '#F9FAFC',
    'datahub-white': colors.white || '#FFFFFF',
    'datahub-success': colors.green?.[500] || '#77B750',
    'datahub-warning': colors.yellow?.[500] || '#EEAE09',
    'datahub-error': colors.red?.[500] || '#CD0D24',
    'datahub-border': colors.gray?.[1400] || '#E9EAEE',
  };
  
  // Add CSS variables
  Object.entries(colorMappings).forEach(([varName, value]) => {
    cssVars += `  --${varName}: ${value};\n`;
  });
  
  // Add shadows and other design tokens
  cssVars += `  --datahub-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);\n`;
  cssVars += `  --datahub-shadow-hover: 0 2px 8px rgba(83, 63, 209, 0.15);\n`;
  cssVars += `  --datahub-node-width: 320px;\n`;
  cssVars += `  --datahub-node-height: 90px;\n`;
  cssVars += `  --datahub-transformation-size: 40px;\n`;
  cssVars += `}\n\n`;
  
  // Dark mode variables
  cssVars += `/* Dark mode colors */\n[data-theme='dark'] {\n`;
  const darkMappings = {
    'datahub-primary': colors.primary?.[400] || colors.violet?.[400] || '#7565DA',
    'datahub-primary-dark': colors.primary?.[500] || colors.violet?.[500] || '#533FD1',
    'datahub-primary-light': colors.primary?.[300] || colors.violet?.[300] || '#8C7EE0',
    'datahub-primary-lightest': colors.primary?.[800] || colors.violet?.[800] || '#2E2373',
    'datahub-gray-100': colors.gray?.[700] || '#2F3657',
    'datahub-gray-600': colors.gray?.[200] || '#CFD1DA',
    'datahub-gray-1700': colors.gray?.[300] || '#A9ADBD',
    'datahub-gray-1800': colors.gray?.[400] || '#81879F',
    'datahub-gray-1500': colors.gray?.[2000] || '#1E2338',
    'datahub-white': colors.gray?.[800] || '#272D48',
    'datahub-border': colors.gray?.[600] || '#374066',
  };
  
  Object.entries(darkMappings).forEach(([varName, value]) => {
    cssVars += `  --${varName}: ${value};\n`;
  });
  
  cssVars += `}\n\n`;
  
  return cssVars;
}

/**
 * Update component CSS files with new design tokens
 */
function updateComponentStyles(cssVariables) {
  const componentDirs = ['DataHubEntityCard', 'DataHubLineageNode'];
  
  componentDirs.forEach(componentDir => {
    const styleFile = path.join(DOCS_COMPONENTS_DIR, componentDir, 'styles.module.css');
    
    try {
      let content = fs.readFileSync(styleFile, 'utf8');
      
      // Replace the CSS variables section
      const variableRegex = /\/\* Auto-generated DataHub Design Tokens \*\/[\s\S]*?}\s*\n\s*\n/;
      
      if (variableRegex.test(content)) {
        content = content.replace(variableRegex, cssVariables);
      } else {
        // If no existing variables section, add at the top
        content = cssVariables + content;
      }
      
      fs.writeFileSync(styleFile, content);
      console.log(`✅ Updated ${componentDir} styles`);
      
    } catch (error) {
      console.error(`❌ Failed to update ${componentDir}:`, error.message);
    }
  });
}

/**
 * Main execution
 */
function main() {
  console.log('🔄 Syncing DataHub styles...\n');
  
  const colors = extractDataHubColors();
  const semanticTokens = extractSemanticTokens();
  
  if (!colors) {
    console.warn('⚠️  Could not extract DataHub colors from source files.');
    console.log('   Using fallback design tokens to ensure build continues...\n');
    
    // Use fallback colors to ensure build doesn't fail
    const fallbackColors = {
      primary: { 500: '#533FD1', 600: '#4C39BE', 400: '#7565DA', 0: '#F1F3FD' },
      gray: { 100: '#EBECF0', 600: '#374066', 1700: '#5F6685', 1800: '#8088A3', 1500: '#F9FAFC' },
      white: '#FFFFFF',
      green: { 500: '#77B750' },
      yellow: { 500: '#EEAE09' },
      red: { 500: '#CD0D24' }
    };
    
    const cssVariables = generateCSSVariables(fallbackColors, null);
    updateComponentStyles(cssVariables);
    
    console.log('✅ Applied fallback styling - components will use default DataHub colors');
    return;
  }
  
  console.log('📊 Extracted DataHub design tokens');
  console.log(`   - Colors: ${Object.keys(colors).length} palettes`);
  console.log(`   - Semantic tokens: ${semanticTokens ? Object.keys(semanticTokens).length : 0} mappings\n`);
  
  const cssVariables = generateCSSVariables(colors, semanticTokens);
  updateComponentStyles(cssVariables);
  
  console.log('\n🎉 DataHub styles sync completed!');
  console.log('   Tutorial components now match the latest DataHub UI styling.');
}

// Run the script
if (require.main === module) {
  main();
}

module.exports = { extractDataHubColors, generateCSSVariables, updateComponentStyles };
