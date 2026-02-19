#!/usr/bin/env node

/**
 * Icon Import Validation Script
 *
 * Validates that:
 * 1. No files import directly from '@phosphor-icons/react' (except type-only imports)
 * 2. All imported icons are exported in the custom icon bundle
 *
 * Usage: node scripts/validate-icon-imports.js
 */

const fs = require('fs');
const path = require('path');
const glob = require('glob');

console.log('\n=== Icon Import Validation ===\n');

// Step 1: Check for direct imports from @phosphor-icons/react
const files = glob.sync('src/**/*.{ts,tsx}', {
    cwd: process.cwd(),
    absolute: true,
    ignore: [
        '**/node_modules/**',
        '**/dist/**',
        '**/build/**',
        '**/phosphor-icons.ts', // Our custom bundle is allowed
        '**/__tests__/**', // Ignore test files
        '**/*.test.ts',
        '**/*.test.tsx',
    ],
});

let hasDirectImports = false;
let problemFiles = [];

files.forEach((file) => {
    const content = fs.readFileSync(file, 'utf8');

    // Check for direct imports from @phosphor-icons/react
    // Allow type-only imports (they don't affect bundle size)
    const directImportMatch = content.match(/import\s+(?!type\s).*from ['"]@phosphor-icons\/react['"]/);

    if (directImportMatch) {
        hasDirectImports = true;
        const relativePath = path.relative(path.join(__dirname, '..'), file);
        problemFiles.push(relativePath);
    }
});

if (hasDirectImports) {
    console.error('✗ Found direct imports from @phosphor-icons/react:\n');
    console.error('These files should import from custom icon bundle:\n');
    problemFiles.forEach((file) => console.error(`  - ${file}`));
    console.error('\nExpected: import { Icon } from "@components/Icon/phosphor-icons"');
    console.error('Found:    import { Icon } from "@phosphor-icons/react"');
    console.error('\nRun: node scripts/update-phosphor-imports.js\n');
    process.exit(1);
}

console.log('✓ All icon imports use custom bundle');
console.log(`  Checked ${files.length} files\n`);

// Step 2: Verify all imported icons are exported
console.log('=== Verifying Icon Exports ===\n');

const phosphorIconsPath = path.join(__dirname, '..', 'src/alchemy-components/components/Icon/phosphor-icons.ts');
const phosphorIconsContent = fs.readFileSync(phosphorIconsPath, 'utf8');

// Extract exported icons
const exportedIcons = new Set();
const exportMatches = phosphorIconsContent.matchAll(/export\s+\{\s*(\w+)\s*\}/g);
for (const match of exportMatches) {
    exportedIcons.add(match[1]);
}

console.log(`✓ Found ${exportedIcons.size} exported icons in custom bundle\n`);

// Extract all imported icons from source files
const importedIcons = new Set();
const missingIcons = new Set();

files.forEach((file) => {
    const content = fs.readFileSync(file, 'utf8');

    // Match imports like: import { Icon1, Icon2 } from '@components/components/Icon/phosphor-icons'
    const importRegex = /import\s+\{([^}]+)\}\s+from\s+['"]@components\/components\/Icon\/phosphor-icons['"]/g;
    const matches = content.matchAll(importRegex);

    for (const match of matches) {
        const importList = match[1];
        // Split by comma and clean up each icon name
        const icons = importList
            .split(',')
            .map((i) => i.trim())
            .filter((i) => i && i !== 'Icon' && i !== 'PhosphorIcons');
        icons.forEach((icon) => {
            importedIcons.add(icon);
            if (!exportedIcons.has(icon)) {
                missingIcons.add(icon);
            }
        });
    }
});

if (missingIcons.size > 0) {
    console.error('✗ Missing icon exports detected:\n');
    console.error(`The following ${missingIcons.size} icons are imported but not exported:\n`);
    Array.from(missingIcons)
        .sort()
        .forEach((icon) => {
            console.error(`  - ${icon}`);
        });
    console.error(`\nAdd these to ${phosphorIconsPath}:`);
    Array.from(missingIcons)
        .sort()
        .forEach((icon) => {
            console.error(`export { ${icon} } from '@phosphor-icons/react/dist/csr/${icon}';`);
        });
    console.error('');
    process.exit(1);
}

console.log('✓ All imported icons are properly exported');
console.log(`  ${importedIcons.size} unique icons imported across codebase\n`);

// Step 3: Check for dynamic icon references (runtime string-based lookups)
console.log('=== Checking Dynamic Icon References ===\n');

const dynamicIconRefs = new Set();
const dynamicMissingIcons = new Set();

files.forEach((file) => {
    const content = fs.readFileSync(file, 'utf8');

    // Pattern 1: <Icon icon="IconName" source="phosphor" />
    // Only check icons that explicitly use source="phosphor"
    const iconPropMatches = content.matchAll(/icon=["']([A-Z][a-zA-Z]+)["']/g);
    for (const match of iconPropMatches) {
        const iconName = match[1];
        // Check if this line also has source="phosphor"
        const lineStart = Math.max(0, match.index - 100);
        const lineEnd = Math.min(content.length, match.index + 100);
        const line = content.substring(lineStart, lineEnd);

        if (line.includes('source="phosphor"') || line.includes("source='phosphor'")) {
            dynamicIconRefs.add(iconName);
            if (!exportedIcons.has(iconName)) {
                dynamicMissingIcons.add(iconName);
            }
        }
    }

    // Pattern 2: phosphorIcons['IconName'] or phosphorIcons[iconName] in utils.ts
    const phosphorLookupMatches = content.matchAll(/phosphorIcons\[["']?([A-Z][a-zA-Z]+)["']?\]/g);
    for (const match of phosphorLookupMatches) {
        const iconName = match[1];
        dynamicIconRefs.add(iconName);
        if (!exportedIcons.has(iconName)) {
            dynamicMissingIcons.add(iconName);
        }
    }
});

if (dynamicMissingIcons.size > 0) {
    console.error('✗ Missing icons detected in dynamic references:\n');
    console.error(`The following ${dynamicMissingIcons.size} icons are referenced dynamically but not exported:\n`);
    Array.from(dynamicMissingIcons)
        .sort()
        .forEach((icon) => {
            console.error(`  - ${icon}`);
        });
    console.error(`\nAdd these to ${phosphorIconsPath}:`);
    Array.from(dynamicMissingIcons)
        .sort()
        .forEach((icon) => {
            console.error(`export { ${icon} } from '@phosphor-icons/react/dist/csr/${icon}';`);
        });
    console.error('');
    process.exit(1);
}

console.log('✓ All dynamic icon references are properly exported');
console.log(`  ${dynamicIconRefs.size} unique icons referenced dynamically\n`);
