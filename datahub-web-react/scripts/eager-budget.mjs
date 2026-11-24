import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import zlib from 'zlib';

export const FORBIDDEN_EAGER_CHUNK = /phosphor-vendor|mui-vendor|mui-icons-catalog/;

/** Soft warn above this; below it size noise is ignored. */
export function toleranceBytes(baselineGzip) {
    return Math.max(10_240, Math.round(baselineGzip * 0.005));
}

/** Hard-fail cliff: growth past 10% of the baseline fails CI (keeps soft warns bounded). */
export function cliffBytes(baselineGzip) {
    return Math.round(baselineGzip * 0.1);
}

/**
 * @typedef {{ hard: string[], soft: string[] }} BudgetFindings
 */

function isFiniteNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

/** @returns {BudgetFindings} */
export function emptyFindings() {
    return { hard: [], soft: [] };
}

/**
 * Classify a gzip delta against baseline: within slack (ok), soft warn, or hard cliff.
 * Shrinks past slack are always soft (ratchet the committed baseline down in the chore).
 *
 * @param {object} args
 * @param {string} args.label human subject, e.g. "Eager JS" or "Logged-in search JS"
 * @param {number} args.actual
 * @param {number} args.expected
 * @param {string} args.ratchetHint how to lower the committed baseline
 * @param {string} [args.overageDetail] appended to soft/hard overage messages
 * @param {BudgetFindings} args.findings
 */
export function classifyGzipDelta({ label, actual, expected, ratchetHint, overageDetail = '', findings }) {
    const slack = toleranceBytes(expected);
    const cliff = cliffBytes(expected);
    const delta = actual - expected;
    if (delta > cliff) {
        findings.hard.push(
            `${label} is ${actual} bytes gzip, ${delta} over the ${expected} baseline ` +
                `(soft warn after +${slack}; hard fail after +${cliff} / 10%).${overageDetail}`,
        );
    } else if (delta > slack) {
        findings.soft.push(
            `${label} is ${actual} bytes gzip, ${delta} over the ${expected} baseline (allowed +${slack}). ` +
                `Trim the graph or raise the baseline in the bundle-budget chore ` +
                `(see datahub-web-react/AGENTS.md, Bundle size budgets).${overageDetail}`,
        );
    } else if (-delta > slack) {
        findings.soft.push(
            `${label} fell to ${actual} bytes gzip, ${-delta} under the ${expected} baseline. ${ratchetHint}`,
        );
    }
}

/**
 * Byte overage past slack (and under the 10% cliff) is soft. Over the cliff is hard.
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
        classifyGzipDelta({
            label: 'Eager JS',
            actual: report.totalGzip,
            expected: baseline.totalGzip,
            ratchetHint: `Update datahub-web-react/eager-budget.json totalGzip to ${report.totalGzip} so the lower size becomes the new ceiling.`,
            overageDetail: ' This is the sum of <script> and modulepreload JS in dist/index.html.',
            findings,
        });
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
 * Soft findings never fail the job. Hard findings do, unless `--json`
 * (Pages comment path must always exit 0).
 *
 * @param {boolean} json
 * @param {BudgetFindings} findings
 */
export function budgetExitCode(json, findings) {
    if (json || findings.hard.length === 0) {
        return 0;
    }
    return 1;
}

/** Escape a message for a GitHub Actions workflow command. */
function escapeWorkflowMessage(message) {
    return message.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}

/**
 * Soft findings: Actions `::warning::` annotations on CI, console.warn locally.
 * Hard findings: stderr (and `::error::` on CI).
 *
 * @param {BudgetFindings} findings
 */
export function printBudgetFindings(findings) {
    const onActions = Boolean(process.env.GITHUB_ACTIONS);
    for (const message of findings.soft) {
        if (onActions) {
            console.warn(`::warning title=Bundle budget::${escapeWorkflowMessage(message)}`);
        } else {
            console.warn(`WARNING (bundle budget, non-blocking): ${message}`);
        }
    }
    for (const message of findings.hard) {
        if (onActions) {
            console.error(`::error title=Bundle budget::${escapeWorkflowMessage(message)}`);
        } else {
            console.error(message);
        }
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
            `Eager JS ${report.totalGzip} bytes gzip (baseline ${baseline.totalGzip}, slack ${toleranceBytes(baseline.totalGzip)}, cliff ${cliffBytes(baseline.totalGzip)})`,
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
