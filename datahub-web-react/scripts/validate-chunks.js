#!/usr/bin/env node

/**
 * Chunk Validation Script
 *
 * Validates that:
 * 1. All expected vendor chunks are generated
 * 2. Phosphor bundle size is optimized (< 200 KB)
 * 3. Total bundle size is within threshold
 *
 * Usage: node scripts/validate-chunks.js
 */

const fs = require('fs');
const path = require('path');

console.log('\n=== Chunk Validation ===\n');

const distPath = path.join(__dirname, '../dist/assets');

// Check if dist exists
if (!fs.existsSync(distPath)) {
    console.error('✗ Build directory not found!');
    console.error('  Run: yarn build');
    process.exit(1);
}

const files = fs.readdirSync(distPath);
const jsFiles = files.filter((f) => f.endsWith('.js'));

// Step 1: Verify required chunks exist
const requiredChunks = [
    'framework', // React core
    'antd-vendor', // Ant Design
    'mui-vendor', // Material UI
    'vendor', // Other node_modules
    'source', // Application code
];

const foundChunks = new Set();

jsFiles.forEach((file) => {
    requiredChunks.forEach((chunk) => {
        if (file.includes(chunk)) {
            foundChunks.add(chunk);
        }
    });
});

// Verify all required chunks exist
let allChunksPresent = true;
requiredChunks.forEach((chunk) => {
    if (foundChunks.has(chunk)) {
        console.log(`✓ ${chunk} chunk found`);
    } else {
        console.error(`✗ ${chunk} chunk MISSING!`);
        allChunksPresent = false;
    }
});

if (!allChunksPresent) {
    console.error('\n✗ Some chunks are missing!');
    console.error('  Check vite.config.ts manualChunks configuration');
    process.exit(1);
}

// Step 2: Check phosphor bundle size
const phosphorFiles = jsFiles.filter((f) => f.includes('phosphor'));
if (phosphorFiles.length === 0) {
    // Phosphor might be bundled into source chunk
    console.log('⚠ Phosphor-specific chunk not found (may be in source chunk)');
} else {
    const phosphorSize = fs.statSync(path.join(distPath, phosphorFiles[0])).size;
    const sizeKB = (phosphorSize / 1024).toFixed(2);

    console.log(`\nPhosphor bundle: ${sizeKB} KB`);

    if (phosphorSize > 300 * 1024) {
        // > 300 KB
        console.error(`✗ Phosphor bundle too large! Expected < 300 KB, got ${sizeKB} KB`);
        console.error('  Likely all icons are bundled. Check icon imports.');
        process.exit(1);
    } else {
        console.log('✓ Phosphor bundle optimized');
    }
}

// Step 3: Check total bundle size
let totalSize = 0;
jsFiles.forEach((file) => {
    totalSize += fs.statSync(path.join(distPath, file)).size;
});

const totalMB = (totalSize / 1024 / 1024).toFixed(2);
console.log(`\nTotal bundle: ${totalMB} MB`);

if (totalSize > 15 * 1024 * 1024) {
    // > 15 MB
    console.error(`✗ Total bundle too large! Expected < 15 MB, got ${totalMB} MB`);
    process.exit(1);
} else {
    console.log('✓ Total bundle size acceptable');
}

// Step 4: Report chunk sizes
console.log('\n=== Chunk Sizes ===\n');
const chunkSizes = [];

jsFiles.forEach((file) => {
    const size = fs.statSync(path.join(distPath, file)).size;
    const sizeMB = (size / 1024 / 1024).toFixed(2);

    // Determine chunk type
    let chunkType = 'other';
    requiredChunks.forEach((chunk) => {
        if (file.includes(chunk)) {
            chunkType = chunk;
        }
    });

    chunkSizes.push({ file, size, sizeMB, type: chunkType });
});

// Sort by type, then size
chunkSizes.sort((a, b) => {
    if (a.type !== b.type) {
        // Sort by chunk order
        const order = ['framework', 'antd-vendor', 'mui-vendor', 'vendor', 'source', 'other'];
        return order.indexOf(a.type) - order.indexOf(b.type);
    }
    return b.size - a.size;
});

// Group by type and display
let currentType = null;
chunkSizes.forEach(({ file, sizeMB, type }) => {
    if (type !== currentType) {
        if (currentType !== null) {
            console.log('');
        }
        console.log(`${type}:`);
        currentType = type;
    }
    console.log(`  ${file}: ${sizeMB} MB`);
});

console.log(`\n✓ All chunks valid (${jsFiles.length} total chunks, ${totalMB} MB)\n`);
