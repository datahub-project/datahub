import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { gzipSize } from './eager-budget.mjs';
import {
    evaluatePageBudget,
    formatBundleComment,
    measureDist,
    measureManifest,
    moduleScriptSrc,
} from './page-bundle-size.mjs';

const sizes = {
    'assets/index.js': 10,
    'assets/shared.js': 100,
    'assets/AuthenticatedApp.js': 20,
    'assets/AppProviders.js': 200,
    'assets/protected.js': 5,
    'assets/SearchRoutes.js': 30,
    'assets/SearchPage.js': 40,
    'assets/search-shared.js': 50,
    'assets/EntityPage.js': 999,
    'assets/HomePage.js': 60,
    'assets/home-shared.js': 70,
    'assets/HomePageV2.js': 888,
    'assets/lazy-icon.js': 777,
};

function sizeOf(file) {
    const size = sizes[file];
    assert.equal(typeof size, 'number', `unexpected file ${file}`);
    return size;
}

// No isEntry. AuthenticatedApp and SearchRoutes are shared chunks: hashed keys, no src.
// SearchPage and homeV3 keep a src. homeV2 is also named HomePage, so name is not enough.
const manifest = {
    'index.html': {
        file: 'assets/index.js',
        src: 'index.html',
        imports: ['_shared.js'],
        dynamicImports: ['_AuthenticatedApp-abc.js'],
    },
    '_shared.js': {
        file: 'assets/shared.js',
        name: 'shared',
    },
    '_AuthenticatedApp-abc.js': {
        file: 'assets/AuthenticatedApp.js',
        name: 'AuthenticatedApp',
        isDynamicEntry: true,
        imports: ['_shared.js', '_AppProviders-def.js', '_protected.js'],
        dynamicImports: ['src/app/homeV2/HomePage.tsx'],
    },
    '_AppProviders-def.js': {
        file: 'assets/AppProviders.js',
        name: 'AppProviders',
        imports: ['_shared.js'],
        dynamicImports: ['assets/lazy-icon.js'],
    },
    '_protected.js': {
        file: 'assets/protected.js',
        imports: ['_shared.js'],
        dynamicImports: ['_SearchRoutes-ghi.js', 'src/app/homeV3/HomePage.tsx'],
    },
    '_SearchRoutes-ghi.js': {
        file: 'assets/SearchRoutes.js',
        name: 'SearchRoutes',
        isDynamicEntry: true,
        imports: ['_shared.js'],
        dynamicImports: ['src/app/searchV2/SearchPage.tsx', 'src/app/entityV2/EntityPage.tsx'],
    },
    'src/app/searchV2/SearchPage.tsx': {
        file: 'assets/SearchPage.js',
        src: 'src/app/searchV2/SearchPage.tsx',
        isDynamicEntry: true,
        imports: ['_search-shared.js'],
    },
    '_search-shared.js': {
        file: 'assets/search-shared.js',
        imports: ['_shared.js'],
    },
    'src/app/entityV2/EntityPage.tsx': {
        file: 'assets/EntityPage.js',
        src: 'src/app/entityV2/EntityPage.tsx',
        isDynamicEntry: true,
    },
    'src/app/homeV3/HomePage.tsx': {
        file: 'assets/HomePage.js',
        src: 'src/app/homeV3/HomePage.tsx',
        isDynamicEntry: true,
        imports: ['_home-shared.js'],
    },
    '_home-shared.js': {
        file: 'assets/home-shared.js',
    },
    'src/app/homeV2/HomePage.tsx': {
        file: 'assets/HomePageV2.js',
        src: 'src/app/homeV2/HomePage.tsx',
        isDynamicEntry: true,
    },
    'assets/lazy-icon.js': {
        file: 'assets/lazy-icon.js',
        isDynamicEntry: true,
    },
};

test('counts a shared chunk once and does not follow dynamic imports', () => {
    const measured = measureManifest(manifest, 'assets/index.js', sizeOf);

    assert.equal(measured.indexHtml, 110);
    assert.deepEqual(measured.files.indexHtml.sort(), ['assets/index.js', 'assets/shared.js']);

    // shared.js is imported by the entry, AuthenticatedApp, AppProviders, and protected.
    assert.equal(measured.shell, 10 + 100 + 20 + 200 + 5);
    assert.equal(measured.files.shell.filter((file) => file === 'assets/shared.js').length, 1);
    for (const excluded of [
        'assets/SearchRoutes.js',
        'assets/SearchPage.js',
        'assets/HomePage.js',
        'assets/HomePageV2.js',
        'assets/EntityPage.js',
        'assets/lazy-icon.js',
    ]) {
        assert.equal(measured.files.shell.includes(excluded), false, excluded);
    }

    assert.equal(measured.search, measured.shell + 30 + 40 + 50);
    assert.equal(measured.files.search.includes('assets/EntityPage.js'), false);
    assert.equal(measured.files.search.includes('assets/HomePage.js'), false);
    assert.equal(measured.files.search.filter((file) => file === 'assets/shared.js').length, 1);

    assert.equal(measured.home, measured.shell + 60 + 70);
    assert.equal(measured.files.home.includes('assets/HomePageV2.js'), false);
    assert.equal(measured.files.home.includes('assets/SearchPage.js'), false);
    assert.equal(measured.appProviders, 200);
});

test('a missing route is n/a and does not fail the other rows', () => {
    const withoutSearch = { ...manifest };
    delete withoutSearch['src/app/searchV2/SearchPage.tsx'];

    const measured = measureManifest(withoutSearch, 'assets/index.js', sizeOf);
    assert.equal(measured.search, null);
    assert.equal(measured.shell, 335);
    assert.equal(measured.home, 335 + 60 + 70);
    assert.equal(measured.indexHtml, 110);
});

test('module script is the entry when the manifest has no isEntry', () => {
    const html = `<!doctype html><html><head>
        <script>(function(){var actor="x";})();</script>
        <script type="module" crossorigin src="./assets/index.js"></script>
        <link rel="modulepreload" crossorigin href="assets/shared.js">
        </head><body></body></html>`;
    assert.equal(moduleScriptSrc(html), 'assets/index.js');

    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'page-bundle-'));
    fs.mkdirSync(path.join(dir, '.vite'));
    fs.mkdirSync(path.join(dir, 'assets'));
    fs.writeFileSync(path.join(dir, 'assets', 'index.js'), 'entry');
    fs.writeFileSync(path.join(dir, 'assets', 'shared.js'), 'shared-chunk');
    fs.writeFileSync(path.join(dir, 'index.html'), html);
    fs.writeFileSync(
        path.join(dir, '.vite', 'manifest.json'),
        JSON.stringify({
            'index.html': { file: 'assets/index.js', src: 'index.html', imports: ['_shared.js'] },
            '_shared.js': { file: 'assets/shared.js' },
        }),
    );

    const measured = measureDist(dir);
    assert.equal(measured.indexHtml, gzipSize(path.join(dir, 'assets', 'index.js')) + gzipSize(path.join(dir, 'assets', 'shared.js')));
    assert.equal(measured.indexHtml, measured.indexHtmlEagerBytes);
    assert.equal(measured.shell, null);
    assert.equal(measured.search, null);
    assert.equal(measured.home, null);
});

test('page budget accepts shell and search within the slack', () => {
    const errors = evaluatePageBudget({ shell: 1_000_000, search: 1_100_000 }, { shell: 1_000_000, search: 1_100_000 });
    assert.deepEqual(errors, []);
});

test('page budget fails when search grows past the slack', () => {
    const errors = evaluatePageBudget({ shell: 1_000_000, search: 1_200_000 }, { shell: 1_000_000, search: 1_100_000 });
    assert.equal(errors.length, 1);
    assert.match(errors[0], /Logged-in search JS is 1200000 bytes gzip/);
});

test('page budget fails when the shell shrinks, so the ceiling must ratchet down', () => {
    const errors = evaluatePageBudget({ shell: 800_000, search: 1_100_000 }, { shell: 1_000_000, search: 1_100_000 });
    assert.match(errors[0], /Update datahub-web-react\/page-budget.json shell to 800000/);
});

test('page budget fails closed when a gated route is missing', () => {
    const errors = evaluatePageBudget({ shell: null, search: 1_100_000 }, { shell: 1_000_000, search: 1_100_000 });
    assert.match(errors[0], /page-budget.json shell/);
});

test('comment table has four rows and n/a when a route is missing', () => {
    const md = formatBundleComment({
        baseSha: '0fb78374ebcf92cfdda3c5d8dee98e40b4981dd6',
        base: { indexHtml: 698 * 1024, shell: 3000 * 1024, search: null, home: 3099 * 1024 },
        pr: { indexHtml: 698 * 1024, shell: 1800 * 1024, search: 2050 * 1024, home: 2690 * 1024 },
    });
    assert.match(md, /## Initial JS bundle size ➡️/);
    assert.match(md, /Base \(`0fb7837`\)/);
    assert.match(md, /\| index\.html \(\/login\) \| 698 KB \| 698 KB \| \*\*\+0 KB \(\+0\.0%\)\*\* \|/);
    assert.match(md, /\| Logged-in shell \|/);
    assert.match(md, /\| \/search \| n\/a \| 2\.00 MB \| \*\*n\/a\*\* \|/);
    assert.match(md, /\| \/ \(home\) \|/);
    assert.match(md, /index\.html is the login page; logged-in pages add the shell/);
});
