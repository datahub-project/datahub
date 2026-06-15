/**
 * CI parity check that keeps non-English locale files in sync with the `en` source of truth.
 *
 * For every locale under src/i18n/locales (other than `en`) it reports three classes of drift:
 *
 *   1. Missing keys      — present in `en`, absent in the locale. The UI silently falls back to
 *                          English, so this is a WARNING by default (does not fail CI). Adding an
 *                          EN-only string in a PR should not be blocked on having translations yet.
 *   2. Extra keys/files  — present in the locale, absent in `en`. These are stale/dead weight left
 *                          behind by deleted or renamed EN keys. Classified as an ERROR.
 *   3. Placeholder drift — a key exists in both, but the `{{varName}}` interpolation variables in the
 *                          translation don't match EN. These break at runtime, so they're ERRORs.
 *
 * Errors only fail CI in `fail` mode (see I18N_PARITY_MODE below); otherwise they're reported only.
 *
 * i18next pluralization is handled key-group-aware: a leaf like `rowCount_one` / `rowCount_other`
 * (and context+plural forms like `hiddenDependencies_downstream_one`) is grouped by stripping the
 * trailing CLDR plural suffix. A locale is only flagged for a missing/extra *group*, never for
 * having a different set of plural forms than English — different languages have different plural
 * rules (e.g. pt-BR may need `_many` where EN has only `_one`/`_other`).
 *
 * Enforcement is controlled by the I18N_PARITY_MODE env var:
 *   - `warn` (default) — report everything but always exit 0 (never blocks CI).
 *   - `fail`           — exit 1 when there are errors (extra/stale keys, placeholder drift).
 * Missing keys are always warnings regardless of mode (the UI falls back to English).
 *
 * Usage:
 *   node scripts/check-i18n-parity.mjs                 # check all non-EN locales (warn-only)
 *   node scripts/check-i18n-parity.mjs --lang de       # check a single locale
 *   I18N_PARITY_MODE=fail node scripts/check-i18n-parity.mjs   # block CI on errors
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

// Whether errors should fail CI. Defaults to warn-only so EN-only PRs aren't blocked while
// translations catch up; flip via `I18N_PARITY_MODE=fail` (in CI step env) to enforce.
const mode = (process.env.I18N_PARITY_MODE || 'warn').toLowerCase();
const enforce = ['fail', 'error', 'strict', 'true', '1'].includes(mode);

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

// Newlines in a workflow-command message must be encoded so the whole annotation stays on one line.
function encodeAnnotation(message) {
    return message.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}

// Emit one annotation per locale per severity so they appear on the run/PR checks page. One
// annotation per (locale, severity) keeps us well under GitHub's per-run display cap. In warn-only
// mode, errors are downgraded to warning-level annotations so a non-blocking run isn't shown with
// red error markers on an otherwise-green check.
function emitGithubAnnotations(perLocale, errorsBlock) {
    if (process.env.GITHUB_ACTIONS !== 'true') return;
    const errorLevel = errorsBlock ? 'error' : 'warning';
    for (const { lang, langWarnings, langErrors } of perLocale) {
        if (langWarnings.length > 0) {
            const body = langWarnings.map((w) => `⚠️ ${w}`).join('\n');
            console.log(`::warning title=i18n parity: ${lang} (${langWarnings.length})::${encodeAnnotation(body)}`);
        }
        if (langErrors.length > 0) {
            const body = langErrors.map((e) => `❌ ${e}`).join('\n');
            console.log(`::${errorLevel} title=i18n parity: ${lang} (${langErrors.length})::${encodeAnnotation(body)}`);
        }
    }
}

// Build the markdown report shared by the job summary and the sticky PR comment.
function buildSummaryMarkdown(perLocale, warnings, errors) {
    const lines = ['## i18n locale parity', ''];
    const status = errors > 0 ? '❌ failed' : warnings > 0 ? '⚠️ passed with warnings' : '✅ in sync';
    lines.push(`**Status:** ${status} — ${warnings} warning(s), ${errors} error(s)`, '');
    lines.push('| Locale | Missing (warn) | Errors |', '| --- | --- | --- |');
    for (const { lang, langWarnings, langErrors } of perLocale) {
        lines.push(`| ${lang} | ${langWarnings.length} | ${langErrors.length} |`);
    }
    lines.push('');

    for (const { lang, langWarnings, langErrors } of perLocale) {
        if (langWarnings.length === 0 && langErrors.length === 0) continue;
        const detail = [...langErrors.map((e) => `❌ ${e}`), ...langWarnings.map((w) => `⚠️ ${w}`)].join('\n');
        lines.push(`<details><summary>${lang}</summary>`, '', '```', detail, '```', '', '</details>', '');
    }
    return lines.join('\n');
}

// Render the report to the job summary page (visible on the run page without opening logs) and,
// when there are findings, to a file the workflow turns into a sticky PR comment.
function writeReports(perLocale, warnings, errors) {
    const markdown = buildSummaryMarkdown(perLocale, warnings, errors);
    if (process.env.GITHUB_STEP_SUMMARY) {
        appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${markdown}\n`);
    }
    // Only emit the comment file when there's something to report — absence signals "in sync" to
    // the workflow so it doesn't open a noisy comment on every PR.
    if (process.env.I18N_PARITY_SUMMARY_FILE && warnings + errors > 0) {
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

let totalWarnings = 0;
let totalErrors = 0;
const results = [];

for (const lang of languages) {
    const langDir = path.join(localesDir, lang);
    const langWarnings = [];
    const langErrors = [];

    // Whole namespace files that exist in the locale but not in EN are stale dead weight.
    const extraFiles = jsonFilesIn(langDir).filter((f) => !namespaces.includes(f));
    for (const f of extraFiles) {
        langErrors.push(`${f} — file does not exist in "${BASE_LANG}" (stale, should be deleted)`);
    }

    for (const nsFile of namespaces) {
        const enFlat = loadFlat(path.join(enDir, nsFile));
        const enKeys = Object.keys(enFlat);
        const otherPath = path.join(langDir, nsFile);

        if (!existsSync(otherPath)) {
            // Entire namespace untranslated — treated as missing keys (always a warning).
            langWarnings.push(`${nsFile} — file missing entirely (${enKeys.length} untranslated key(s))`);
            continue;
        }

        const otherFlat = loadFlat(otherPath);
        const otherKeys = Object.keys(otherFlat);

        const enGroups = new Set(enKeys.map(groupKey));
        const otherGroups = new Set(otherKeys.map(groupKey));

        const missingGroups = [...enGroups].filter((g) => !otherGroups.has(g));
        const extraGroups = [...otherGroups].filter((g) => !enGroups.has(g));

        if (missingGroups.length > 0) {
            langWarnings.push(
                `${nsFile} — ${missingGroups.length} missing key(s):\n${missingGroups.map((k) => `      - ${k}`).join('\n')}`,
            );
        }

        if (extraGroups.length > 0) {
            langErrors.push(
                `${nsFile} — ${extraGroups.length} extra/stale key(s):\n${extraGroups.map((k) => `      - ${k}`).join('\n')}`,
            );
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
            langErrors.push(`${nsFile} — ${mismatches.length} placeholder mismatch(es):\n${mismatches.join('\n')}`);
        }
    }

    console.log(`\n──────── ${lang} ────────`);
    if (langWarnings.length === 0 && langErrors.length === 0) {
        console.log('  ✅  in sync with en');
    }
    for (const w of langWarnings) console.warn(`  ⚠️  ${w}`);
    for (const e of langErrors) console.error(`  ❌  ${e}`);

    results.push({ lang, langWarnings, langErrors });
    totalWarnings += langWarnings.length;
    totalErrors += langErrors.length;
}

// Surface results in the GitHub Actions UI (annotations + job summary) so they're visible on the
// run/PR checks page without opening the raw job logs. No-ops when run outside Actions.
emitGithubAnnotations(results, enforce);
writeReports(results, totalWarnings, totalErrors);

console.log(`\n────────────────────────`);
console.log(`Locales checked: ${languages.join(', ')}`);
console.log(`Mode: ${enforce ? 'fail' : 'warn'}   Warnings: ${totalWarnings}   Errors: ${totalErrors}`);

if (totalErrors > 0) {
    if (enforce) {
        console.error(
            `\n❌  i18n parity check failed with ${totalErrors} error(s).` +
                ` Fix extra/stale keys and placeholder mismatches above.`,
        );
        process.exit(1);
    }
    console.warn(
        `\n⚠️  i18n parity found ${totalErrors} error(s) (extra/stale keys, placeholder drift),` +
            ` reporting only. Set I18N_PARITY_MODE=fail to block CI on these.`,
    );
}

if (totalWarnings > 0) {
    console.warn(`\n⚠️  ${totalWarnings} missing-translation warning(s). Missing keys fall back to English.`);
} else if (totalErrors === 0) {
    console.log('\n✅  All locales are in sync with en.');
}

process.exit(0);
