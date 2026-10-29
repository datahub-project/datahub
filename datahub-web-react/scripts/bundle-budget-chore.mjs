#!/usr/bin/env node
/**
 * Decide what the scheduled bundle-budget reassessment should do.
 *
 * Reads dist/eager-budget-findings.json and dist/page-budget-findings.json after
 * a master build, then:
 *   - ratchet-down: lower only fields whose measured gzip is under the committed
 *     baseline (never raise a field that grew within slack)
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

export function readJson(filePath) {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

export function isUnderBaseline(message) {
    return /fell to .+ under the .+ baseline/.test(message);
}

/** @param {{ soft?: string[], hard?: string[] } | null | undefined} findings */
export function classify(findings) {
    const soft = findings?.soft || [];
    const hard = findings?.hard || [];
    if (hard.length) return 'hard';
    if (!soft.length) return 'ok';
    if (soft.every(isUnderBaseline)) return 'ratchet-down';
    return 'soft-over';
}

/**
 * Never raise a committed ceiling. Each field becomes min(baseline, measured).
 * Fields without a measured value are left alone.
 *
 * @param {{ eager: { totalGzip: number }, page: { shell: number, search: number }, eagerMeasured: number, pageMeasured: { shell?: number | null, search?: number | null } }} args
 */
export function planRatchet({ eager, page, eagerMeasured, pageMeasured }) {
    return {
        eager: {
            ...eager,
            totalGzip: Math.min(eager.totalGzip, eagerMeasured),
        },
        page: {
            ...page,
            shell: pageMeasured.shell == null ? page.shell : Math.min(page.shell, pageMeasured.shell),
            search:
                pageMeasured.search == null ? page.search : Math.min(page.search, pageMeasured.search),
        },
    };
}

export function budgetsChanged(before, after) {
    return (
        before.eager.totalGzip !== after.eager.totalGzip ||
        before.page.shell !== after.page.shell ||
        before.page.search !== after.page.search
    );
}

export function applyRatchet({ eagerMeasured, pageMeasured, rootDir = root }) {
    const eagerPath = path.join(rootDir, 'eager-budget.json');
    const pagePath = path.join(rootDir, 'page-budget.json');
    const eager = JSON.parse(fs.readFileSync(eagerPath, 'utf8'));
    const page = JSON.parse(fs.readFileSync(pagePath, 'utf8'));
    const planned = planRatchet({ eager, page, eagerMeasured, pageMeasured });
    if (!budgetsChanged({ eager, page }, planned)) {
        return { eager, page, changed: false };
    }
    fs.writeFileSync(eagerPath, `${JSON.stringify(planned.eager, null, 4)}\n`);
    fs.writeFileSync(pagePath, `${JSON.stringify(planned.page, null, 4)}\n`);
    return { eager: planned.eager, page: planned.page, changed: true };
}

function main() {
    const apply = process.argv.includes('--apply');
    const eagerFindingsPath = path.join(distDir, 'eager-budget-findings.json');
    const pageFindingsPath = path.join(distDir, 'page-budget-findings.json');
    const eagerFindingsRaw = readJson(eagerFindingsPath);
    const pageFindingsRaw = readJson(pageFindingsPath);
    if (!eagerFindingsRaw) {
        console.warn(`WARNING: missing ${eagerFindingsPath}; treating eager budget as ok`);
    }
    if (!pageFindingsRaw) {
        console.warn(`WARNING: missing ${pageFindingsPath}; treating page budget as ok`);
    }
    const eagerFindings = eagerFindingsRaw || { soft: [], hard: [] };
    const pageFindings = pageFindingsRaw || { soft: [], hard: [], measured: {} };
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
