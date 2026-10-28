import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import zlib from 'zlib';

export const FORBIDDEN_EAGER_CHUNK = /phosphor-vendor|mui-vendor|mui-icons-catalog/;

export function toleranceBytes(baselineGzip) {
    return Math.max(10_240, Math.round(baselineGzip * 0.005));
}

/**
 * @typedef {{ hard: string[], soft: string[] }} BudgetFindings
 */

function isFiniteNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

function emptyFindings() {
    return { hard: [], soft: [] };
}

/** Flatten for callers that only need messages (tests, printing). Soft first, then hard. */
export function allBudgetMessages(findings) {
    return [...findings.soft, ...findings.hard];
}

/**
 * Byte overage / under-baseline ratchet are soft (warn, do not fail CI).
 * Composition denylist and broken baselines stay hard.
 *
 * @param {{ totalGzip: number, chunks: { file: string, phosphorModules: number, muiIconModules: number }[] }} report
 * @param {{ totalGzip: number, maxPhosphorModules: number }} baseline
 * @returns {BudgetFindings}
 */
export function evaluateEagerBudget(report, baseline) {
    const findings = emptyFindings();
    if (!isFiniteNumber(baseline.totalGzip) || !isFiniteNumber(report.totalGzip)) {
        findings.hard.push(
            'eager-budget.json totalGzip must be a finite number, and the report must include one. ' +
                'Without it the size comparison does not run.',
        );
    } else {
        const slack = toleranceBytes(baseline.totalGzip);
        const delta = report.totalGzip - baseline.totalGzip;

        if (delta > slack) {
            findings.soft.push(
                `Eager JS is ${report.totalGzip} bytes gzip, ${delta} over the ${baseline.totalGzip} baseline (allowed +${slack}). ` +
                    `This is the sum of <script> and modulepreload JS in dist/index.html. ` +
                    `Byte overage warns; it does not fail the build. Raise or trim via the bundle-budget chore.`,
            );
        } else if (-delta > slack) {
            findings.soft.push(
                `Eager JS fell to ${report.totalGzip} bytes gzip, ${-delta} under the ${baseline.totalGzip} baseline. ` +
                    `Update datahub-web-react/eager-budget.json totalGzip to ${report.totalGzip} so the lower size becomes the new ceiling.`,
            );
        }
    }

    for (const chunk of report.chunks) {
        if (FORBIDDEN_EAGER_CHUNK.test(chunk.file)) {
            findings.hard.push(
                `${chunk.file} is an eager catalog chunk. Named vendor chunks load with the first page.`,
            );
        }
        if (chunk.muiIconModules > 0) {
            findings.hard.push(
                `${chunk.file} eagerly includes ${chunk.muiIconModules} @mui/icons-material modules. ` +
                    `That catalog must stay in an async chunk.`,
            );
        }
    }

    const phosphorModules = report.chunks.reduce((sum, chunk) => sum + chunk.phosphorModules, 0);
    if (!isFiniteNumber(baseline.maxPhosphorModules)) {
        findings.hard.push('eager-budget.json maxPhosphorModules must be a finite number.');
    } else if (phosphorModules > baseline.maxPhosphorModules) {
        findings.hard.push(
            `Eager JS includes ${phosphorModules} Phosphor modules (ceiling ${baseline.maxPhosphorModules}). ` +
                `The shell may import the icons it renders; the rest of the catalog must stay off index.html.`,
        );
    }

    return findings;
}

// Node zlib at level 9. The gzip CLI and CPython compress the same bytes to a
// different size, so the committed baseline has to be this function's output.
export function gzipSize(filePath) {
    return zlib.gzipSync(fs.readFileSync(filePath), { level: 9 }).length;
}

export function readReport(distDir) {
    const reportPath = path.join(distDir, 'eager-budget-report.json');
    const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
    let totalGzip = 0;
    for (const chunk of report.chunks) {
        const filePath = path.join(distDir, chunk.file);
        chunk.gzip = gzipSize(filePath);
        totalGzip += chunk.gzip;
    }
    return { totalGzip, chunks: report.chunks };
}

// react-cloudflare-pages.yml parses `yarn size --json` as size-limit did: a JSON
// array whose first object's `size` is the gzip byte count. Stdout must be that
// array alone; the human summary would make json.load fail.
export function sizeLimitJson(totalGzip) {
    return [{ size: totalGzip }];
}

/**
 * Soft byte findings never fail the job. Hard composition / config findings do,
 * unless `--json` (Pages comment path must always exit 0).
 *
 * @param {boolean} json
 * @param {BudgetFindings | string[]} findingsOrHard
 */
export function budgetExitCode(json, findingsOrHard) {
    if (json) {
        return 0;
    }
    const hard = Array.isArray(findingsOrHard) ? findingsOrHard : findingsOrHard.hard;
    return hard.length === 0 ? 0 : 1;
}

export function printBudgetFindings(findings) {
    for (const message of findings.soft) {
        console.warn(`WARNING (bundle budget, non-blocking): ${message}`);
    }
    for (const message of findings.hard) {
        console.error(message);
    }
}

function main() {
    const json = process.argv.includes('--json');
    const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
    const baseline = JSON.parse(fs.readFileSync(path.join(root, 'eager-budget.json'), 'utf8'));
    const report = readReport(path.join(root, 'dist'));
    const findings = evaluateEagerBudget(report, baseline);
    if (json) {
        console.log(JSON.stringify(sizeLimitJson(report.totalGzip)));
    } else {
        const lines = report.chunks.map(
            (chunk) =>
                `  ${chunk.gzip}  ${chunk.file}  phosphor=${chunk.phosphorModules} muiIcons=${chunk.muiIconModules}`,
        );
        console.log(
            `Eager JS ${report.totalGzip} bytes gzip (baseline ${baseline.totalGzip}, slack ${toleranceBytes(baseline.totalGzip)})`,
        );
        console.log(lines.join('\n'));
    }
    printBudgetFindings(findings);
    const exitCode = budgetExitCode(json, findings);
    if (exitCode !== 0) {
        process.exit(exitCode);
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main();
}
