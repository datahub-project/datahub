#!/usr/bin/env node
/**
 * Decide what the scheduled bundle-budget reassessment should do.
 *
 * Reads dist/eager-budget-findings.json and dist/page-budget-findings.json after
 * a master build, then:
 *   - ratchet-down: rewrite eager-budget.json / page-budget.json to measured
 *     values when the only soft findings are under-baseline shrinks
 *   - soft-over: report only (do not bump ceilings)
 *   - hard: exit 1
 *   - ok: nothing to do
 *
 * Usage (from repo root, after frontend build + budget scripts):
 *   node datahub-web-react/scripts/bundle-budget-chore.mjs [--apply]
 */
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

import { readReport } from './eager-budget.mjs';

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const distDir = path.join(root, 'dist');

function readJson(filePath) {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function isUnderBaseline(message) {
    return /fell to .+ under the .+ baseline/.test(message);
}

function classify(findings) {
    const soft = findings?.soft || [];
    const hard = findings?.hard || [];
    if (hard.length) return 'hard';
    if (!soft.length) return 'ok';
    if (soft.every(isUnderBaseline)) return 'ratchet-down';
    return 'soft-over';
}

function applyRatchet({ eagerMeasured, pageMeasured }) {
    const eagerPath = path.join(root, 'eager-budget.json');
    const pagePath = path.join(root, 'page-budget.json');
    const eager = JSON.parse(fs.readFileSync(eagerPath, 'utf8'));
    const page = JSON.parse(fs.readFileSync(pagePath, 'utf8'));
    const nextEager = { ...eager, totalGzip: eagerMeasured };
    const nextPage = {
        ...page,
        ...(pageMeasured.shell != null ? { shell: pageMeasured.shell } : {}),
        ...(pageMeasured.search != null ? { search: pageMeasured.search } : {}),
    };
    fs.writeFileSync(eagerPath, `${JSON.stringify(nextEager, null, 4)}\n`);
    fs.writeFileSync(pagePath, `${JSON.stringify(nextPage, null, 4)}\n`);
    return { eager: nextEager, page: nextPage };
}

function main() {
    const apply = process.argv.includes('--apply');
    const eagerFindings = readJson(path.join(distDir, 'eager-budget-findings.json')) || {
        soft: [],
        hard: [],
    };
    const pageFindings = readJson(path.join(distDir, 'page-budget-findings.json')) || {
        soft: [],
        hard: [],
        measured: {},
    };
    const eagerClass = classify(eagerFindings);
    const pageClass = classify(pageFindings);
    const order = { hard: 3, 'soft-over': 2, 'ratchet-down': 1, ok: 0 };
    const decision =
        [eagerClass, pageClass].sort((a, b) => order[b] - order[a])[0] || 'ok';

    const report = readReport(distDir);
    const summary = {
        decision,
        eagerClass,
        pageClass,
        eagerMeasured: report.totalGzip,
        pageMeasured: pageFindings.measured || {},
        eagerSoft: eagerFindings.soft,
        eagerHard: eagerFindings.hard,
        pageSoft: pageFindings.soft,
        pageHard: pageFindings.hard,
    };

    if (decision === 'ratchet-down' && apply) {
        summary.applied = applyRatchet({
            eagerMeasured: report.totalGzip,
            pageMeasured: pageFindings.measured || {},
        });
    }

    console.log(JSON.stringify(summary, null, 2));
    if (decision === 'hard') {
        process.exit(1);
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main();
}
