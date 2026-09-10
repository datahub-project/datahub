#!/usr/bin/env node
// Generates a thin re-export stub for every Phosphor CSR icon into lazy-icons/.
// Vite builds each stub as a separate async chunk so the main bundle stays icon-free —
// end users only download the specific icons an admin configured, not all 1500+.
// Run via the Gradle task generateLazyIconStubs, or: node scripts/generate-lazy-icon-stubs.js

const fs = require('fs');
const path = require('path');

const csrDir = path.resolve(__dirname, '../node_modules/@phosphor-icons/react/dist/csr');
const outDir = path.resolve(__dirname, '../src/app/mfeframework/lazy-icons');

const iconNames = fs
    .readdirSync(csrDir)
    .filter((f) => f.endsWith('.mjs'))
    .map((f) => f.slice(0, -4)); // strip .mjs

fs.mkdirSync(outDir, { recursive: true });

for (const name of iconNames) {
    fs.writeFileSync(
        path.join(outDir, `${name}.ts`),
        `export { ${name} } from '@phosphor-icons/react/dist/csr/${name}';\n`,
    );
}

console.log(`[generate-lazy-icon-stubs] ${iconNames.length} stubs → ${path.relative(process.cwd(), outDir)}`);
