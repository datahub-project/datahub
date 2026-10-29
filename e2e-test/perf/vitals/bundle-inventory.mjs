#!/usr/bin/env node
/**
 * Eager JS bundle inventory for a `vite build` of datahub-web-react.
 *
 * "Eager" is the JS `dist/index.html` loads as a `<script>` or
 * `<link rel="modulepreload">` — the same set `datahub-web-react/scripts/eager-budget.mjs`
 * budgets. Everything else under `dist/assets/*.js` is async.
 *
 * Zero dependencies — node:fs / node:zlib only.
 *
 *   node bundle-inventory.mjs --dist ../../../datahub-web-react/dist \
 *     --graph results/bundle-graph.json --label local --commit <sha> \
 *     --out results/bundle-inventory.json --md-out results/bundle-inventory.md
 *
 *   node bundle-inventory.mjs --origin https://datahub.example.com \
 *     --label main --out results/bundle-inventory-live.json
 *
 *   node bundle-inventory.mjs --dist <dist> --baseline <eager-budget-inventory.json> --regression
 */
import fs from 'node:fs';
import path from 'node:path';

import {
    APP_SOURCE,
    GZIP_LEVEL,
    attributeEager,
    classifyReachability,
    inventoryDist,
    inventoryOrigin,
} from './bundle-inventory-core.mjs';
import {
    diffAgainstBaseline,
    renderMarkdown,
    renderRegression,
} from './bundle-inventory-render.mjs';

export {
    APP_SOURCE,
    appAreaOf,
    attributeChunk,
    attributeEager,
    bearerFor,
    classifyReachability,
    collapseChain,
    eagerJsRefs,
    gzipSize,
    inventoryDist,
    inventoryOrigin,
    packageOf,
    parseIndexHtml,
    resolveAssetUrl,
    shortId,
} from './bundle-inventory-core.mjs';

export {
    REGRESSION_MIN_DELTA,
    chunkRole,
    diffAgainstBaseline,
    notableDeltas,
    renderMarkdown,
    renderRegression,
} from './bundle-inventory-render.mjs';

export function slimAcceptedInventory(attribution) {
    const slim = (rows) => (rows || []).map((r) => ({ package: r.package, gzip_bytes: r.gzip_bytes }));
    return {
        eager_whole_file_gzip_bytes: attribution.eager_whole_file_gzip_bytes,
        packages: slim(attribution.packages),
        app_areas: slim(attribution.app_source_areas),
    };
}

function readEagerModuleCounts(distDir) {
    const reportPath = path.join(distDir, 'eager-budget-report.json');
    if (!fs.existsSync(reportPath)) return null;
    const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
    const chunks = report.chunks || [];
    return {
        phosphorModules: chunks.reduce((sum, chunk) => sum + (chunk.phosphorModules || 0), 0),
        muiIconModules: chunks.reduce((sum, chunk) => sum + (chunk.muiIconModules || 0), 0),
    };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
    const args = { top: 30 };
    for (let i = 0; i < argv.length; i += 1) {
        const a = argv[i];
        const next = () => {
            i += 1;
            return argv[i];
        };
        if (a === '--dist') args.dist = next();
        else if (a === '--graph') args.graph = next();
        else if (a === '--origin') args.origin = next();
        else if (a === '--label') args.label = next();
        else if (a === '--commit') args.commit = next();
        else if (a === '--out') args.out = next();
        else if (a === '--md-out') args.mdOut = next();
        else if (a === '--baseline') args.baseline = next();
        else if (a === '--write-baseline') args.writeBaseline = next();
        else if (a === '--regression') args.regression = true;
        else if (a === '--top') args.top = Number(next());
        else if (a === '--help' || a === '-h') args.help = true;
        else throw new Error(`unknown argument ${a}`);
    }
    return args;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help || (!args.dist && !args.origin)) {
        process.stdout.write(
            [
                'usage: node bundle-inventory.mjs --dist <dir> [--graph <bundle-graph.json>] [--label <name>] [--commit <sha>]',
                '                                 [--baseline <previous-inventory.json>] [--regression] [--write-baseline <file>]',
                '                                 [--top N] [--out <json>] [--md-out <md>]',
                '       node bundle-inventory.mjs --origin <https://host> [--out <json>] [--md-out <md>]',
                '',
                'Build with source maps and the module-graph dump first:',
                '  cd datahub-web-react && DATAHUB_BUNDLE_GRAPH_OUT=../e2e-test/perf/vitals/results/bundle-graph.json \\',
                "    yarn run buildWithSourceMap-only",
                '',
            ].join('\n'),
        );
        process.exit(args.help ? 0 : 2);
    }

    if ((args.regression || args.writeBaseline) && !args.dist) {
        throw new Error('--regression and --write-baseline require --dist');
    }

    const meta = {
        generated_at: new Date().toISOString(),
        label: args.label ?? (args.origin ? args.origin : path.resolve(args.dist)),
        commit: args.commit ?? null,
        gzip: `zlib gzip level ${GZIP_LEVEL}`,
        node: process.version,
    };

    let report;
    if (args.origin) {
        const token = process.env.DATAHUB_GMS_TOKEN || process.env.DATAHUB_ACCESS_TOKEN;
        report = { meta, inventory: await inventoryOrigin(args.origin, { token }) };
    } else {
        const distDir = path.resolve(args.dist);
        const inventory = inventoryDist(distDir);
        const eagerFiles = inventory.eager_js.files.map((f) => f.file);
        const attribution = attributeEager(distDir, eagerFiles);
        let reachability = null;
        if (args.graph) {
            const graph = JSON.parse(fs.readFileSync(path.resolve(args.graph), 'utf8'));
            reachability = classifyReachability(graph, eagerFiles);
        }
        report = { meta, inventory, attribution, reachability };
        if (args.writeBaseline) {
            const slim = slimAcceptedInventory(attribution);
            const baselinePath = path.resolve(args.writeBaseline);
            fs.mkdirSync(path.dirname(baselinePath), { recursive: true });
            fs.writeFileSync(baselinePath, `${JSON.stringify(slim, null, 4)}\n`);
            process.stdout.write(
                `wrote ${baselinePath} (${slim.packages.length} packages, eager gzip ${slim.eager_whole_file_gzip_bytes})\n`,
            );
            return;
        }
        if (args.baseline || args.regression) {
            if (!args.baseline) {
                throw new Error('--regression requires --baseline <eager-budget-inventory.json>');
            }
            const baseline = JSON.parse(fs.readFileSync(path.resolve(args.baseline), 'utf8'));
            const basePackages = baseline.attribution?.packages ?? baseline.packages ?? [];
            const label = baseline.meta?.label ?? path.basename(args.baseline);
            report.baseline = diffAgainstBaseline(attribution.packages, basePackages, label);
            report.areaBaseline = diffAgainstBaseline(
                attribution.app_source_areas,
                baseline.app_areas ?? baseline.attribution?.app_source_areas ?? [],
                label,
            );
            report.acceptedEagerGzip = baseline.eager_whole_file_gzip_bytes ?? null;
        }
    }

    if (args.regression) {
        const counts = readEagerModuleCounts(path.resolve(args.dist));
        const md = renderRegression({
            eagerGzip: report.attribution.eager_whole_file_gzip_bytes,
            baselineGzip: report.acceptedEagerGzip,
            packageDiff: report.baseline,
            areaDiff: report.areaBaseline,
            phosphorModules: counts?.phosphorModules,
            muiIconModules: counts?.muiIconModules,
            top: args.top,
        });
        if (args.mdOut) {
            fs.mkdirSync(path.dirname(path.resolve(args.mdOut)), { recursive: true });
            fs.writeFileSync(args.mdOut, md);
        }
        process.stdout.write(md);
        return;
    }

    const json = `${JSON.stringify(report, null, 2)}\n`;
    if (args.out) {
        fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
        fs.writeFileSync(args.out, json);
    }
    const md = report.inventory.eager_js ? renderMarkdown(report, { top: args.top }) : '';
    if (args.mdOut) {
        fs.mkdirSync(path.dirname(path.resolve(args.mdOut)), { recursive: true });
        fs.writeFileSync(args.mdOut, md);
    }
    process.stdout.write(args.mdOut || args.out ? `${md}\n` : json);
}

const invokedDirectly = process.argv[1] && path.resolve(process.argv[1]) === new URL(import.meta.url).pathname;
if (invokedDirectly) {
    main().catch((err) => {
        process.stderr.write(`${err.stack || err}\n`);
        process.exit(1);
    });
}
