/**
 * CI parity check that keeps non-English locale files in sync with the `en` source of truth.
 *
 * For every locale under src/i18n/locales (other than `en`) it reports three classes of drift:
 *
 *   1. Missing keys      — present in `en`, absent in the locale (the UI falls back to English).
 *   2. Extra keys/files  — present in the locale, absent in `en`: stale/dead weight left behind by
 *                          deleted or renamed EN keys.
 *   3. Placeholder drift — a key exists in both, but the `{{varName}}` interpolation variables in
 *                          the translation don't match EN (these break at runtime).
 *
 * i18next pluralization is handled key-group-aware: a leaf like `rowCount_one` / `rowCount_other`
 * (and context+plural forms like `hiddenDependencies_downstream_one`) is grouped by stripping the
 * trailing CLDR plural suffix. A locale is only flagged for a missing/extra *group*, never for
 * having a different set of plural forms than English — different languages have different plural
 * rules (e.g. pt-BR may need `_many` where EN has only `_one`/`_other`).
 *
 * Enforcement is controlled by the I18N_PARITY_MODE env var:
 *   - `warn` (default) — report every drift but always exit 0 (never blocks CI).
 *   - `fail`           — exit 1 when there is ANY drift, including missing keys.
 *
 * Usage:
 *   node scripts/check-i18n-parity.mjs                 # check all non-EN locales (warn-only)
 *   node scripts/check-i18n-parity.mjs --lang de       # check a single locale
 *   I18N_PARITY_MODE=fail node scripts/check-i18n-parity.mjs   # block CI on any drift
 */
import { appendFileSync, existsSync, readFileSync, readdirSync, statSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const localesDir = path.resolve(__dirname, '../src/i18n/locales');
const BASE_LANG = 'en';

// CLDR plural / ordinal suffixes appended by i18next to plural keys. Stripped when grouping keys so
// that language-specific plural forms aren't mistaken for missing/extra keys.
const PLURAL_SUFFIX = /_(zero|one|two|few|many|other)$/;

const args = process.argv.slice(2);
const langFilter = args.includes('--lang') ? args[args.indexOf('--lang') + 1] : null;

// Whether any drift should fail CI. Defaults to warn-only; flip via `I18N_PARITY_MODE=fail`.
const mode = (process.env.I18N_PARITY_MODE || 'warn').toLowerCase();
const enforce = ['fail', 'error', 'strict', 'true', '1'].includes(mode);
// Every finding is drift that should be fixed, so it's always marked ❌ in the report/comment
// regardless of mode. The mode only controls whether the run blocks (exit code + annotation level).
const ICON = '❌';

function loadFlat(filePath) {
    const flat = {};
    for (const [key, value] of flattenEntries(JSON.parse(readFileSync(filePath, 'utf-8')))) {
        flat[key] = value;
    }
    return flat;
}

function flattenEntries(obj, prefix = '') {
    return Object.entries(obj).flatMap(([key, value]) => {
        const fullKey = prefix ? `${prefix}.${key}` : key;
        return typeof value === 'object' && value !== null ? flattenEntries(value, fullKey) : [[fullKey, value]];
    });
}

// The plural-group identity of a key: its path with the trailing plural suffix removed.
function groupKey(key) {
    return key.replace(PLURAL_SUFFIX, '');
}

// Extract the set of interpolation variable names from a translation value. Handles i18next
// formatting syntax (`{{count, number}}`, `{{owners, list}}`) by taking the name before the comma.
function placeholders(value) {
    if (typeof value !== 'string') return new Set();
    const names = new Set();
    for (const match of value.matchAll(/\{\{([^}]+)\}\}/g)) {
        names.add(match[1].split(',')[0].trim());
    }
    return names;
}

function jsonFilesIn(dir) {
    return readdirSync(dir).filter((f) => f.endsWith('.json') && statSync(path.join(dir, f)).isFile());
}

function countByType(findings, type) {
    return findings.filter((f) => f.type === type).length;
}

// Newlines in a workflow-command message must be encoded so the whole annotation stays on one line.
function encodeAnnotation(message) {
    return message.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}

// Emit one annotation per locale so findings appear on the run/PR checks page without opening logs.
// Severity follows the mode: in `fail` mode drift is an error, in `warn` mode it's a warning.
function emitGithubAnnotations(perLocale) {
    if (process.env.GITHUB_ACTIONS !== 'true') return;
    const level = enforce ? 'error' : 'warning';
    for (const { lang, findings } of perLocale) {
        if (findings.length === 0) continue;
        const body = findings.map((f) => `${ICON} ${f.message}`).join('\n');
        console.log(`::${level} title=i18n parity: ${lang} (${findings.length})::${encodeAnnotation(body)}`);
    }
}

// Build the markdown report shared by the job summary and the sticky PR comment.
function buildSummaryMarkdown(perLocale, total) {
    const lines = ['## i18n locale parity', ''];
    const status = total === 0 ? '✅ in sync' : enforce ? '❌ failed' : '⚠️ drift detected (warn-only)';
    lines.push(`**Status:** ${status} — ${total} issue(s) across ${perLocale.length} locale(s)`, '');
    lines.push('| Locale | Missing | Stale | Placeholder |', '| --- | --- | --- | --- |');
    for (const { lang, findings } of perLocale) {
        lines.push(
            `| ${lang} | ${countByType(findings, 'missing')} | ${countByType(findings, 'stale')} | ${countByType(findings, 'placeholder')} |`,
        );
    }
    lines.push('');

    for (const { lang, findings } of perLocale) {
        if (findings.length === 0) continue;
        const detail = findings.map((f) => `${ICON} ${f.message}`).join('\n');
        lines.push(`<details><summary>${lang}</summary>`, '', '```', detail, '```', '', '</details>', '');
    }
    return lines.join('\n');
}

// Render the report to the job summary page (visible on the run page without opening logs) and,
// when there are findings, to a file the workflow turns into a sticky PR comment.
function writeReports(perLocale, total) {
    const markdown = buildSummaryMarkdown(perLocale, total);
    if (process.env.GITHUB_STEP_SUMMARY) {
        appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${markdown}\n`);
    }
    // Only emit the comment file when there's something to report — absence signals "in sync" to
    // the workflow so it doesn't open a noisy comment on every PR.
    if (process.env.I18N_PARITY_SUMMARY_FILE && total > 0) {
        appendFileSync(process.env.I18N_PARITY_SUMMARY_FILE, `${markdown}\n`);
    }
}

const enDir = path.join(localesDir, BASE_LANG);
const namespaces = jsonFilesIn(enDir);
const languages = readdirSync(localesDir).filter(
    (l) => l !== BASE_LANG && statSync(path.join(localesDir, l)).isDirectory() && (!langFilter || l === langFilter),
);

if (languages.length === 0) {
    console.log(langFilter ? `No locale directory found for "${langFilter}".` : 'No non-EN locales found.');
    process.exit(0);
}

let total = 0;
const results = [];

for (const lang of languages) {
    const langDir = path.join(localesDir, lang);
    const findings = [];

    // Whole namespace files that exist in the locale but not in EN are stale dead weight.
    const extraFiles = jsonFilesIn(langDir).filter((f) => !namespaces.includes(f));
    for (const f of extraFiles) {
        findings.push({
            type: 'stale',
            message: `${f} — file does not exist in "${BASE_LANG}" (stale, should be deleted)`,
        });
    }

    for (const nsFile of namespaces) {
        const enFlat = loadFlat(path.join(enDir, nsFile));
        const enKeys = Object.keys(enFlat);
        const otherPath = path.join(langDir, nsFile);

        if (!existsSync(otherPath)) {
            findings.push({
                type: 'missing',
                message: `${nsFile} — file missing entirely (${enKeys.length} untranslated key(s))`,
            });
            continue;
        }

        const otherFlat = loadFlat(otherPath);
        const otherKeys = Object.keys(otherFlat);

        const enGroups = new Set(enKeys.map(groupKey));
        const otherGroups = new Set(otherKeys.map(groupKey));

        const missingGroups = [...enGroups].filter((g) => !otherGroups.has(g));
        const extraGroups = [...otherGroups].filter((g) => !enGroups.has(g));

        if (missingGroups.length > 0) {
            findings.push({
                type: 'missing',
                message: `${nsFile} — ${missingGroups.length} missing key(s):\n${missingGroups.map((k) => `      - ${k}`).join('\n')}`,
            });
        }

        if (extraGroups.length > 0) {
            findings.push({
                type: 'stale',
                message: `${nsFile} — ${extraGroups.length} extra/stale key(s):\n${extraGroups.map((k) => `      - ${k}`).join('\n')}`,
            });
        }

        // Placeholder drift for keys present in both locales.
        const mismatches = [];
        for (const key of enKeys) {
            if (!(key in otherFlat)) continue;
            const enVars = placeholders(enFlat[key]);
            const otherVars = placeholders(otherFlat[key]);
            const missing = [...enVars].filter((v) => !otherVars.has(v));
            const extra = [...otherVars].filter((v) => !enVars.has(v));
            if (missing.length > 0 || extra.length > 0) {
                const parts = [];
                if (missing.length > 0) parts.push(`missing ${missing.map((v) => `{{${v}}}`).join(', ')}`);
                if (extra.length > 0) parts.push(`unexpected ${extra.map((v) => `{{${v}}}`).join(', ')}`);
                mismatches.push(`      - ${key}: ${parts.join('; ')}`);
            }
        }
        if (mismatches.length > 0) {
            findings.push({
                type: 'placeholder',
                message: `${nsFile} — ${mismatches.length} placeholder mismatch(es):\n${mismatches.join('\n')}`,
            });
        }
    }

    console.log(`\n──────── ${lang} ────────`);
    if (findings.length === 0) {
        console.log('  ✅  in sync with en');
    }
    for (const f of findings) {
        (enforce ? console.error : console.warn)(`  ${ICON}  ${f.message}`);
    }

    results.push({ lang, findings });
    total += findings.length;
}

// Surface results in the GitHub Actions UI (annotations + job summary) so they're visible on the
// run/PR checks page without opening the raw job logs. No-ops when run outside Actions.
emitGithubAnnotations(results);
writeReports(results, total);

console.log(`\n────────────────────────`);
console.log(`Locales checked: ${languages.join(', ')}`);
console.log(`Mode: ${enforce ? 'fail' : 'warn'}   Issues: ${total}`);

if (total === 0) {
    console.log('\n✅  All locales are in sync with en.');
    process.exit(0);
}

if (enforce) {
    console.error(`\n❌  i18n parity check failed with ${total} issue(s). Fix the drift above.`);
    process.exit(1);
}

console.warn(
    `\n⚠️  i18n parity found ${total} issue(s), reporting only. Set I18N_PARITY_MODE=fail to block CI on these.`,
);
process.exit(0);
