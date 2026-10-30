#!/usr/bin/env node
/**
 * Decide what the scheduled bundle-budget reassessment should do.
 *
 * Reads eager/page findings JSON after a default-branch build, then:
 *   - ratchet-down: lower only fields whose own soft finding is an under-baseline
 *     shrink (never raise; each written field is min(baseline, measured))
 *   - soft-over: report only (do not bump ceilings)
 *   - hard: exit 1
 *   - ok: nothing to do
 *
 * Usage (from repo root, after frontend build + budget scripts):
 *   node datahub-web-react/scripts/bundle-budget-chore.mjs [--apply]
 *     [--budget-dir DIR] [--eager-budget FILE] [--page-budget FILE] [--dist DIR]
 */
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

import { resolveBudgetPaths, takeBudgetPathArgs } from './budget-paths.mjs';
import { readReport } from './eager-budget.mjs';

/** Prefer findings.measured (artifact-friendly); fall back to recomputing from dist JS. */
function resolveEagerMeasured(eagerFindings, distDir) {
    const fromFindings = eagerFindings?.measured?.totalGzip;
    if (typeof fromFindings === 'number' && Number.isFinite(fromFindings)) {
        return fromFindings;
    }
    return readReport(distDir).totalGzip;
}

export function readJson(filePath) {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

export function isUnderBaseline(message) {
    return /fell to .+ under the .+ baseline/.test(message);
}

/** Soft under-baseline finding that names this field (label substring). */
export function hasUnderBaselineFieldFinding(softMessages, labelPart) {
    return (softMessages || []).some((message) => isUnderBaseline(message) && message.includes(labelPart));
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
 * Never raise a committed ceiling. Only fields with their own under-baseline
 * soft finding are candidates; each becomes min(baseline, measured).
 *
 * @param {object} args
 * @param {{ totalGzip: number }} args.eager
 * @param {{ shell: number, search: number }} args.page
 * @param {number} args.eagerMeasured
 * @param {{ shell?: number | null, search?: number | null }} args.pageMeasured
 * @param {string[]} [args.eagerSoft]
 * @param {string[]} [args.pageSoft]
 */
export function planRatchet({
    eager,
    page,
    eagerMeasured,
    pageMeasured,
    eagerSoft = [],
    pageSoft = [],
}) {
    const nextEager = { ...eager };
    const nextPage = { ...page };
    if (hasUnderBaselineFieldFinding(eagerSoft, 'Eager JS') && Number.isFinite(eagerMeasured)) {
        nextEager.totalGzip = Math.min(eager.totalGzip, eagerMeasured);
    }
    if (
        hasUnderBaselineFieldFinding(pageSoft, 'shell') &&
        pageMeasured.shell != null &&
        Number.isFinite(pageMeasured.shell)
    ) {
        nextPage.shell = Math.min(page.shell, pageMeasured.shell);
    }
    if (
        hasUnderBaselineFieldFinding(pageSoft, 'search') &&
        pageMeasured.search != null &&
        Number.isFinite(pageMeasured.search)
    ) {
        nextPage.search = Math.min(page.search, pageMeasured.search);
    }
    return { eager: nextEager, page: nextPage };
}

export function budgetsChanged(before, after) {
    return (
        before.eager.totalGzip !== after.eager.totalGzip ||
        before.page.shell !== after.page.shell ||
        before.page.search !== after.page.search
    );
}

/**
 * Pure ratchet: returns the next JSON objects. Caller writes files when changed.
 *
 * @param {object} args
 * @param {object} args.eager committed eager-budget.json
 * @param {object} args.page committed page-budget.json
 * @param {number} args.eagerMeasured
 * @param {{ shell?: number | null, search?: number | null }} args.pageMeasured
 * @param {string[]} [args.eagerSoft]
 * @param {string[]} [args.pageSoft]
 */
export function applyRatchet({ eager, page, eagerMeasured, pageMeasured, eagerSoft, pageSoft }) {
    const planned = planRatchet({
        eager,
        page,
        eagerMeasured,
        pageMeasured,
        eagerSoft,
        pageSoft,
    });
    return {
        eager: planned.eager,
        page: planned.page,
        changed: budgetsChanged({ eager, page }, planned),
    };
}

function writeBudgetJson(filePath, value) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, `${JSON.stringify(value, null, 4)}\n`);
}

function main(argv = process.argv.slice(2)) {
    const { rest, paths: pathOverrides } = takeBudgetPathArgs(argv);
    const apply = rest.includes('--apply');
    const paths = resolveBudgetPaths(pathOverrides);

    const eagerFindingsPath = path.join(paths.dist, 'eager-budget-findings.json');
    const pageFindingsPath = path.join(paths.dist, 'page-budget-findings.json');
    const eagerFindings = readJson(eagerFindingsPath);
    const pageFindings = readJson(pageFindingsPath);
    if (!eagerFindings) {
        console.error(`Missing findings file: ${eagerFindingsPath}`);
        process.exit(1);
    }
    if (!pageFindings) {
        console.error(`Missing findings file: ${pageFindingsPath}`);
        process.exit(1);
    }

    const eagerClass = classify(eagerFindings);
    const pageClass = classify(pageFindings);
    const order = { hard: 3, 'soft-over': 2, 'ratchet-down': 1, ok: 0 };
    const decision = [eagerClass, pageClass].sort((a, b) => order[b] - order[a])[0] || 'ok';

    const eagerMeasured = resolveEagerMeasured(eagerFindings, paths.dist);
    const summary = {
        decision,
        eagerClass,
        pageClass,
        eagerMeasured,
        pageMeasured: pageFindings.measured || {},
        eagerSoft: eagerFindings.soft,
        eagerHard: eagerFindings.hard,
        pageSoft: pageFindings.soft,
        pageHard: pageFindings.hard,
    };

    if (decision === 'ratchet-down' && apply) {
        const eager = JSON.parse(fs.readFileSync(paths.eagerBudget, 'utf8'));
        const page = JSON.parse(fs.readFileSync(paths.pageBudget, 'utf8'));
        const applied = applyRatchet({
            eager,
            page,
            eagerMeasured,
            pageMeasured: pageFindings.measured || {},
            eagerSoft: eagerFindings.soft || [],
            pageSoft: pageFindings.soft || [],
        });
        if (applied.changed) {
            writeBudgetJson(paths.eagerBudget, applied.eager);
            writeBudgetJson(paths.pageBudget, applied.page);
        }
        summary.applied = applied;
    }

    console.log(JSON.stringify(summary, null, 2));
    if (decision === 'hard') {
        process.exit(1);
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main();
}
