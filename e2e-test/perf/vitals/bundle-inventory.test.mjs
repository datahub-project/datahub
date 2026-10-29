import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';
import {
    APP_SOURCE,
    appAreaOf,
    attributeChunk,
    attributeEager,
    bearerFor,
    chunkRole,
    classifyReachability,
    collapseChain,
    diffAgainstBaseline,
    eagerJsRefs,
    notableDeltas,
    renderRegression,
    inventoryDist,
    packageOf,
    parseIndexHtml,
    renderMarkdown,
    resolveAssetUrl,
} from './bundle-inventory.mjs';

const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
function vlq(value) {
    let v = value < 0 ? (-value << 1) | 1 : value << 1;
    let out = '';
    do {
        let digit = v & 31;
        v >>>= 5;
        if (v > 0) digit |= 32;
        out += B64[digit];
    } while (v > 0);
    return out;
}

// One generated line; each segment is [genColDelta, srcIdxDelta, 0, 0].
function mappingsFor(segments) {
    let prevCol = 0;
    let prevSrc = 0;
    return segments
        .map(([col, src]) => {
            const s = `${vlq(col - prevCol)}${vlq(src - prevSrc)}${vlq(0)}${vlq(0)}`;
            prevCol = col;
            prevSrc = src;
            return s;
        })
        .join(',');
}

describe('parseIndexHtml', () => {
    it('lists module scripts, modulepreload links and stylesheets', () => {
        const html = `
          <head>
            <link rel="stylesheet" crossorigin href="assets/index-abc.css">
            <link rel="modulepreload" crossorigin href="assets/pre-def.js">
            <script>window.__X__=1</script>
          </head>
          <body>
            <script type="module" crossorigin src="assets/vendor-111.js"></script>
            <script type="module" crossorigin src="assets/source-222.js"></script>
          </body>`;
        const parsed = parseIndexHtml(html);
        assert.deepEqual(parsed.scripts, ['assets/vendor-111.js', 'assets/source-222.js']);
        assert.deepEqual(parsed.modulepreload, ['assets/pre-def.js']);
        assert.deepEqual(parsed.stylesheets, ['assets/index-abc.css']);
    });
});

describe('eagerJsRefs / resolveAssetUrl / chunkRole / bearerFor', () => {
    const html = `
      <base href="/datahub/">
      <link rel="modulepreload" crossorigin href="assets/vendor-AbCdEf12.js">
      <script type="module" crossorigin src="assets/index-12345678.js"></script>`;

    it('treats modulepreload JS as eager, once', () => {
        const parsed = parseIndexHtml(html);
        assert.deepEqual(eagerJsRefs(parsed).sort(), ['assets/index-12345678.js', 'assets/vendor-AbCdEf12.js']);
        assert.deepEqual(
            eagerJsRefs({ ...parsed, scripts: [...parsed.scripts, parsed.modulepreload[0]] }).sort(),
            ['assets/index-12345678.js', 'assets/vendor-AbCdEf12.js'],
        );
    });

    it('resolves asset refs against base href and leaves absolute URLs alone', () => {
        const page = 'https://example.test/login';
        assert.equal(
            resolveAssetUrl(page, html, 'assets/vendor-AbCdEf12.js'),
            'https://example.test/datahub/assets/vendor-AbCdEf12.js',
        );
        assert.equal(
            resolveAssetUrl(page, '<base href="@basePath">', 'assets/index-12345678.js'),
            'https://example.test/assets/index-12345678.js',
        );
        assert.equal(
            resolveAssetUrl(page, html, 'https://cdn.example.test/assets/index-12345678.js'),
            'https://cdn.example.test/assets/index-12345678.js',
        );
    });

    it('keeps hyphenated chunk names and only strips the trailing hash', () => {
        assert.equal(chunkRole('mui-icons-catalog-AbCdEf12.js'), 'mui-icons-catalog');
        assert.equal(chunkRole('react-dom-xyz12345.js'), 'react-dom');
        assert.equal(chunkRole('vendor-BhgU6n3S.js'), 'vendor');
    });

    it('sends the bearer token only to the measured origin', () => {
        assert.equal(bearerFor('https://deploy-test.example/assets/vendor.js', 'https://deploy-test.example', 'tok'), 'Bearer tok');
        assert.equal(bearerFor('https://idp.example/authorize', 'https://deploy-test.example', 'tok'), null);
        assert.equal(bearerFor('https://deploy-test.example/login', 'https://deploy-test.example', null), null);
    });
});

describe('attributeEager', () => {
    it('counts an eager file with no source map in the total and warns', () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bundle-attr-'));
        fs.mkdirSync(path.join(dir, 'assets'));
        fs.writeFileSync(path.join(dir, 'assets', 'vendor-12345678.js'), 'x'.repeat(100));
        const warnings = [];
        const write = process.stderr.write;
        process.stderr.write = (chunk, ...rest) => {
            warnings.push(String(chunk));
            return write.call(process.stderr, chunk, ...rest);
        };
        try {
            const result = attributeEager(dir, ['vendor-12345678.js']);
            assert.equal(result.chunks[0].source_map, false);
            assert.equal(result.eager_whole_file_gzip_bytes, result.chunks[0].gzip_bytes);
            assert.equal(result.packages.length, 0);
            assert.match(warnings.join(''), /no source map for eager file vendor-12345678.js/);
        } finally {
            process.stderr.write = write;
        }
    });
});

describe('packageOf / appAreaOf', () => {
    it('collapses node_modules paths to package names, keeping scopes', () => {
        assert.equal(packageOf('../../node_modules/lodash/map.js'), 'lodash');
        assert.equal(packageOf('/w/datahub-web-react/node_modules/@mui/material/Button/index.js'), '@mui/material');
        assert.equal(packageOf('/w/node_modules/a/node_modules/b/index.js'), 'b');
    });
    it('treats src/ and datahub_agents as first-party', () => {
        assert.equal(packageOf('../../src/app/entityV2/Foo.tsx'), APP_SOURCE);
        assert.equal(packageOf('../../../datahub_agents/canary/frontend/index.tsx'), APP_SOURCE);
        assert.equal(appAreaOf('../../src/app/entityV2/Foo.tsx'), 'src/app/entityV2');
        assert.equal(appAreaOf('../../src/graphql/dataset.generated.ts'), 'src/graphql');
        assert.equal(appAreaOf('../../src/index.tsx'), 'src');
        assert.equal(appAreaOf('../../../datahub_agents/canary/frontend/index.tsx'), 'datahub_agents/canary');
    });
    it('passes virtual modules through under other: but sees through commonjs proxy ids', () => {
        assert.equal(packageOf('\0virtual:i18n-locale-loaders'), 'other:\0virtual:i18n-locale-loaders');
        assert.equal(packageOf('\0/w/node_modules/refractor/core.js?commonjs-proxy'), 'refractor');
        assert.equal(packageOf('/w/src/images/logo.svg?react'), APP_SOURCE);
    });
});

describe('attributeChunk', () => {
    it('attributes generated byte ranges to packages and gzips each blob once', () => {
        const code = Buffer.from('AAAAAAAAAA' + 'BBBBB' + 'CCCCCCCCCCCCCCC' + '\n//# sourceMappingURL=x', 'latin1');
        const sources = ['../../node_modules/lodash/map.js', '../../src/app/Foo.tsx', '../../node_modules/lodash/x.js'];
        const map = { sources, mappings: mappingsFor([[0, 0], [10, 1], [15, 2]]) };
        const { rows, segments } = attributeChunk(code, map);
        assert.equal(segments, 3);
        const lodash = rows.get('lodash');
        const app = rows.get(APP_SOURCE);
        assert.equal(app.raw_bytes, 5);
        assert.equal(app.blob.toString(), 'BBBBB');
        // lodash gets the leading As plus the trailing Cs and the unmapped tail of the last segment
        assert.equal(lodash.blob.toString('latin1').startsWith('AAAAAAAAAACCCCCCCCCCCCCCC'), true);
        assert.equal(lodash.source_file_count, 2);
        assert.ok(lodash.gzip_bytes > 0);
    });
});

describe('classifyReachability', () => {
    it('separates statically reachable modules from import()-only modules pinned into eager chunks', () => {
        const graph = {
            modules: [
                { id: '/w/src/index.tsx', isEntry: true, importedIds: ['/w/src/App.tsx'], dynamicallyImportedIds: [] },
                {
                    id: '/w/src/App.tsx',
                    isEntry: false,
                    importedIds: ['/w/node_modules/react/index.js'],
                    dynamicallyImportedIds: ['/w/src/lazy/Page.tsx'],
                },
                { id: '/w/node_modules/react/index.js', isEntry: false, importedIds: [], dynamicallyImportedIds: [] },
                {
                    id: '/w/src/lazy/Page.tsx',
                    isEntry: false,
                    importedIds: ['/w/node_modules/elkjs/lib/elk.bundled.js'],
                    dynamicallyImportedIds: [],
                },
                { id: '/w/node_modules/elkjs/lib/elk.bundled.js', isEntry: false, importedIds: [], dynamicallyImportedIds: [] },
            ],
            chunks: [
                {
                    fileName: 'assets/vendor-1.js',
                    name: 'vendor',
                    isEntry: false,
                    isDynamicEntry: false,
                    imports: [],
                    dynamicImports: [],
                    modules: { '/w/node_modules/react/index.js': 100, '/w/node_modules/elkjs/lib/elk.bundled.js': 900 },
                },
                {
                    fileName: 'assets/source-1.js',
                    name: 'source',
                    isEntry: true,
                    isDynamicEntry: false,
                    imports: ['assets/vendor-1.js'],
                    dynamicImports: [],
                    modules: { '/w/src/index.tsx': 10, '/w/src/App.tsx': 20, '/w/src/lazy/Page.tsx': 30 },
                },
            ],
        };
        const result = classifyReachability(graph, ['vendor-1.js', 'source-1.js']);
        assert.equal(result.static_reachable_module_count, 3);
        const elk = result.packages.find((p) => p.package === 'elkjs');
        assert.equal(elk.dynamic_only_rendered_bytes, 900);
        assert.equal(elk.dynamic_only_share, 1);
        assert.equal(elk.dynamic_only_samples[0].dynamic_entry.module, 'src/lazy/Page.tsx');
        assert.deepEqual(elk.dynamic_only_samples[0].dynamic_entry.importers, ['src/App.tsx']);
        const react = result.packages.find((p) => p.package === 'react');
        assert.equal(react.dynamic_only_rendered_bytes, 0);
        assert.deepEqual(react.app_static_importers, ['src/App.tsx']);
        assert.deepEqual(react.static_chain, ['src/App.tsx', 'react']);
        assert.equal(elk.static_chain, null);
        const app = result.packages.find((p) => p.package === APP_SOURCE);
        assert.equal(app.dynamic_only_rendered_bytes, 30);
    });
});

describe('collapseChain', () => {
    it('keeps the last first-party module and one hop per package', () => {
        const chain = [
            '/w/src/index.tsx',
            '/w/src/App.tsx',
            '/w/src/alchemy-components/components/CodeBlock/CodeBlock.tsx',
            '/w/node_modules/react-syntax-highlighter/dist/esm/index.js',
            '/w/node_modules/react-syntax-highlighter/dist/esm/light.js',
            '/w/node_modules/refractor/index.js',
        ];
        assert.deepEqual(collapseChain(chain), [
            'src/alchemy-components/components/CodeBlock/CodeBlock.tsx',
            'react-syntax-highlighter',
            'refractor',
        ]);
    });
});

describe('inventoryDist + renderMarkdown', () => {
    it('splits eager from async by index.html and renders a report', () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bundle-inv-'));
        fs.mkdirSync(path.join(dir, 'assets'));
        fs.writeFileSync(
            path.join(dir, 'index.html'),
            '<link rel="stylesheet" href="assets/index-a.css">' +
                '<link rel="modulepreload" href="assets/shared-c.js">' +
                '<script type="module" src="assets/source-a.js"></script>',
        );
        fs.writeFileSync(path.join(dir, 'assets', 'source-a.js'), 'x'.repeat(1000));
        fs.writeFileSync(path.join(dir, 'assets', 'shared-c.js'), 'w'.repeat(200));
        fs.writeFileSync(path.join(dir, 'assets', 'Lazy-b.js'), 'y'.repeat(500));
        fs.writeFileSync(path.join(dir, 'assets', 'index-a.css'), 'z'.repeat(100));
        const inv = inventoryDist(dir);
        assert.equal(inv.eager_js.count, 2);
        assert.equal(inv.eager_js.raw_bytes, 1200);
        assert.equal(inv.async_js.count, 1);
        assert.equal(inv.async_js.raw_bytes, 500);
        assert.equal(inv.eager_css.raw_bytes, 100);
        assert.equal(inv.all_js.raw_bytes, 1700);
        const md = renderMarkdown({ meta: { label: 'test', commit: null, generated_at: 'now' }, inventory: inv });
        assert.match(md, /\| Eager JS \| 2 \| 1,200 \|/);
        assert.match(md, /`source-a.js`/);
    });
});

describe('diffAgainstBaseline', () => {
    it('reports additions, removals and growth sorted by absolute delta', () => {
        const now = [
            { package: 'a', gzip_bytes: 100 },
            { package: 'b', gzip_bytes: 50 },
        ];
        const before = [
            { package: 'a', gzip_bytes: 10 },
            { package: 'c', gzip_bytes: 300 },
        ];
        const { deltas } = diffAgainstBaseline(now, before, 'march');
        assert.deepEqual(
            deltas.map((d) => [d.package, d.delta]),
            [
                ['c', -300],
                ['a', 90],
                ['b', 50],
            ],
        );
    });
});

describe('renderRegression', () => {
    it('keeps package and area moves past the noise floor and names an example source', () => {
        const packageDiff = diffAgainstBaseline(
            [
                { package: 'react-dom', gzip_bytes: 5_000, sample_sources: ['react-dom/client.js'] },
                { package: 'scheduler', gzip_bytes: 100, sample_sources: ['scheduler/index.js'] },
            ],
            [
                { package: 'react-dom', gzip_bytes: 1_000 },
                { package: 'scheduler', gzip_bytes: 80 },
            ],
            'accepted',
        );
        const areaDiff = diffAgainstBaseline(
            [{ package: 'src/app/entityV2', gzip_bytes: 9_000, sample_sources: ['src/app/entityV2/chart/ChartEntity.tsx'] }],
            [{ package: 'src/app/entityV2', gzip_bytes: 8_000 }],
            'accepted',
        );
        const md = renderRegression({
            eagerGzip: 730_000,
            baselineGzip: 725_000,
            packageDiff,
            areaDiff,
            phosphorModules: 92,
            muiIconModules: 0,
        });
        assert.match(md, /Eager gzip is 730,000/);
        assert.match(md, /`react-dom`/);
        assert.match(md, /\+4,000/);
        assert.match(md, /`src\/app\/entityV2`/);
        assert.match(md, /ChartEntity\.tsx/);
        assert.match(md, /Phosphor modules in eager chunks: 92/);
        assert.equal(notableDeltas(packageDiff.deltas).some((d) => d.package === 'scheduler'), false);
    });
});
