/**
 * Bundle Optimization Tests (Phase 1)
 *
 * Validates the Phosphor icons bundle optimization:
 * 1. Bundle size stays within thresholds
 * 2. All icon imports use the custom bundle
 * 3. Required chunks are generated
 * 4. No missing icons at runtime
 */
import fs from 'fs';
import glob from 'glob';
import path from 'path';
import { describe, expect, test } from 'vitest';

describe('Phase 1: Bundle Optimization', () => {
    const projectRoot = path.join(__dirname, '../..');
    const phosphorIconsPath = path.join(projectRoot, 'src/alchemy-components/components/Icon/phosphor-icons.ts');

    describe('Icon Import Validation', () => {
        test('no files should import directly from @phosphor-icons/react', () => {
            const files = glob.sync('src/**/*.{ts,tsx}', {
                cwd: projectRoot,
                absolute: true,
                ignore: [
                    '**/node_modules/**',
                    '**/dist/**',
                    '**/build/**',
                    '**/phosphor-icons.ts', // Our custom bundle is allowed
                ],
            });

            const problemFiles: string[] = [];

            files.forEach((file) => {
                const content = fs.readFileSync(file, 'utf8');

                // Check for direct imports (excluding type-only imports)
                const directImportMatch = content.match(/import\s+(?!type\s).*from ['"]@phosphor-icons\/react['"]/);

                if (directImportMatch) {
                    problemFiles.push(path.relative(projectRoot, file));
                }
            });

            if (problemFiles.length > 0) {
                const fileList = problemFiles.map((f) => `  - ${f}`).join('\n');
                throw new Error(
                    `Found ${problemFiles.length} files with direct @phosphor-icons/react imports:\n${fileList}\n\nExpected: import from "@components/components/Icon/phosphor-icons"`,
                );
            }

            expect(problemFiles).toHaveLength(0);
        });

        test('all imported icons should be exported in custom bundle', () => {
            const phosphorIconsContent = fs.readFileSync(phosphorIconsPath, 'utf8');

            // Extract exported icons
            const exportedIcons = new Set<string>();
            const exportMatches = Array.from(phosphorIconsContent.matchAll(/export\s+\{\s*(\w+)\s*\}/g));
            exportMatches.forEach((match) => {
                exportedIcons.add(match[1]);
            });

            // Extract all imported icons from source files
            const files = glob.sync('src/**/*.{ts,tsx}', {
                cwd: projectRoot,
                absolute: true,
                ignore: ['**/__tests__/**', '**/*.test.ts', '**/*.test.tsx'],
            });

            const missingIcons = new Set<string>();

            files.forEach((file) => {
                const content = fs.readFileSync(file, 'utf8');

                // Match imports like: import { Icon1, Icon2 } from '@components/components/Icon/phosphor-icons'
                const importRegex =
                    /import\s+\{([^}]+)\}\s+from\s+['"]@components\/components\/Icon\/phosphor-icons['"]/g;
                const matches = Array.from(content.matchAll(importRegex));

                matches.forEach((match) => {
                    const importList = match[1];
                    const icons = importList
                        .split(',')
                        .map((i) => i.trim())
                        // Filter out non-icon imports
                        .filter((i) => {
                            if (!i) return false;
                            // Exclude these specific non-icon exports
                            const excludeList = ['Icon', 'PhosphorIcons', 'IconNames', 'PhosphorIconType'];
                            return !excludeList.includes(i);
                        });

                    icons.forEach((icon) => {
                        if (!exportedIcons.has(icon)) {
                            missingIcons.add(icon);
                        }
                    });
                });
            });

            if (missingIcons.size > 0) {
                const missingList = Array.from(missingIcons)
                    .sort()
                    .map((icon) => `export { ${icon} } from '@phosphor-icons/react/dist/csr/${icon}';`)
                    .join('\n');

                throw new Error(`Found ${missingIcons.size} missing icon exports:\n\n${missingList}`);
            }

            expect(missingIcons.size).toBe(0);
            expect(exportedIcons.size).toBeGreaterThan(100);
        });

        test('all dynamic icon references should be in custom bundle', () => {
            const phosphorIconsContent = fs.readFileSync(phosphorIconsPath, 'utf8');

            // Extract exported icons
            const exportedIcons = new Set<string>();
            const exportMatches = Array.from(phosphorIconsContent.matchAll(/export\s+\{\s*(\w+)\s*\}/g));
            exportMatches.forEach((match) => {
                exportedIcons.add(match[1]);
            });

            const files = glob.sync('src/**/*.{ts,tsx}', {
                cwd: projectRoot,
                absolute: true,
                ignore: ['**/__tests__/**', '**/*.test.ts', '**/*.test.tsx'],
            });

            const dynamicMissingIcons = new Set<string>();

            files.forEach((file) => {
                const content = fs.readFileSync(file, 'utf8');

                // Pattern: <Icon icon="IconName" source="phosphor" />
                const iconPropMatches = Array.from(content.matchAll(/icon=["']([A-Z][a-zA-Z]+)["']/g));
                iconPropMatches.forEach((match) => {
                    const iconName = match[1];
                    // Check if this line also has source="phosphor"
                    const lineStart = Math.max(0, match.index! - 100);
                    const lineEnd = Math.min(content.length, match.index! + 100);
                    const line = content.substring(lineStart, lineEnd);

                    if (line.includes('source="phosphor"') || line.includes("source='phosphor'")) {
                        if (!exportedIcons.has(iconName)) {
                            dynamicMissingIcons.add(iconName);
                        }
                    }
                });
            });

            if (dynamicMissingIcons.size > 0) {
                const missingList = Array.from(dynamicMissingIcons)
                    .sort()
                    .map((icon) => `export { ${icon} } from '@phosphor-icons/react/dist/csr/${icon}';`)
                    .join('\n');

                throw new Error(`Found ${dynamicMissingIcons.size} missing dynamic icon references:\n\n${missingList}`);
            }

            expect(dynamicMissingIcons.size).toBe(0);
        });
    });

    describe('Custom Icon Bundle', () => {
        test('custom bundle should export expected number of icons', () => {
            const content = fs.readFileSync(phosphorIconsPath, 'utf8');

            const exportMatches = content.matchAll(/export\s+\{\s*\w+\s*\}/g);
            const exportCount = Array.from(exportMatches).length;

            // Phase 1: ~123 icons (vs 4,539 in full package)
            expect(exportCount).toBeGreaterThanOrEqual(100);
            expect(exportCount).toBeLessThan(200);
        });

        test('custom bundle should use CSR imports for tree-shaking', () => {
            const content = fs.readFileSync(phosphorIconsPath, 'utf8');

            // All imports should be from /dist/csr/ for tree-shaking
            const importMatches = content.matchAll(/from ['"]@phosphor-icons\/react\/dist\/csr\/\w+['"]/g);
            const csrImportCount = Array.from(importMatches).length;

            const exportMatches = content.matchAll(/export\s+\{\s*\w+\s*\}/g);
            const exportCount = Array.from(exportMatches).length;

            // Every export should have a corresponding CSR import
            expect(csrImportCount).toBe(exportCount);
        });
    });

    describe('Bundle Size Validation (requires build)', () => {
        const distPath = path.join(projectRoot, 'dist/assets');

        test.skipIf(!fs.existsSync(distPath))('total bundle should be under 15 MB minified', () => {
            const files = fs.readdirSync(distPath);
            const jsFiles = files.filter((f) => f.endsWith('.js'));

            let totalSize = 0;
            jsFiles.forEach((file) => {
                totalSize += fs.statSync(path.join(distPath, file)).size;
            });

            // After Phase 1: ~13 MB (down from 18 MB)
            expect(totalSize).toBeLessThan(15 * 1024 * 1024);
        });
    });

    describe('Chunk Generation (requires build)', () => {
        const distPath = path.join(projectRoot, 'dist/assets');

        test.skipIf(!fs.existsSync(distPath))('should generate expected vendor chunks', () => {
            const files = fs.readdirSync(distPath);
            const jsFiles = files.filter((f) => f.endsWith('.js'));

            const expectedChunks = [
                'framework', // React core
                'antd-vendor', // Ant Design
                'mui-vendor', // Material UI
                'vendor', // Other node_modules
                'source', // Application code
            ];

            const foundChunks = new Set<string>();

            jsFiles.forEach((file) => {
                expectedChunks.forEach((chunk) => {
                    if (file.includes(chunk)) {
                        foundChunks.add(chunk);
                    }
                });
            });

            const missingChunks = expectedChunks.filter((chunk) => !foundChunks.has(chunk));

            if (missingChunks.length > 0) {
                throw new Error(
                    `Missing expected chunks: ${missingChunks.join(', ')}\n` +
                        `Found chunks: ${Array.from(foundChunks).join(', ')}`,
                );
            }

            expect(missingChunks).toHaveLength(0);
        });

        test.skipIf(!fs.existsSync(distPath))('framework chunk should be smaller than vendor chunk', () => {
            const files = fs.readdirSync(distPath);
            const jsFiles = files.filter((f) => f.endsWith('.js'));

            const frameworkFile = jsFiles.find((f) => f.includes('framework'));
            // Find main vendor chunk (starts with "vendor-" not other vendor chunks like "antd-vendor")
            const vendorFile = jsFiles.find((f) => f.startsWith('vendor-'));

            expect(frameworkFile).toBeDefined();
            expect(vendorFile).toBeDefined();

            const frameworkSize = fs.statSync(path.join(distPath, frameworkFile!)).size;
            const vendorSize = fs.statSync(path.join(distPath, vendorFile!)).size;

            // Framework (React) should be smaller than vendor (all other deps)
            expect(frameworkSize).toBeLessThan(vendorSize);
        });
    });
});
