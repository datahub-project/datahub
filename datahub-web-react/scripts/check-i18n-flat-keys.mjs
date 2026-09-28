/**
 * CI check that every locale JSON file under src/i18n/locales, `en` included, is flat: each value
 * is a string stored under a dotted key (`"a.b.c": "..."`), never a nested object or array.
 *
 * i18next resolves both `{"a": {"b": "..."}}` and `{"a.b": "..."}` at runtime, so a nested file
 * works in the app. But tools that compare locale files key by key without flattening them first
 * (stale-key cleanup, translation backfill) treat the two shapes as different keys, so one locale
 * storing a key nested while another stores it flat makes them report wrong results.
 *
 * Usage:
 *   node scripts/check-i18n-flat-keys.mjs
 */
import { readFileSync, readdirSync, statSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webReactDir = path.resolve(__dirname, '..');
const localesDir = path.join(webReactDir, 'src/i18n/locales');

// Paths of nested values, reported at the outermost nested level: `{"a": {"b": {"c": "x"}}}`
// yields `a`, which is the key to flatten.
function nestedKeys(obj) {
    return Object.entries(obj)
        .filter(([, value]) => typeof value === 'object' && value !== null)
        .map(([key]) => key);
}

// Newlines in a workflow-command message must be encoded so the whole annotation stays on one line.
function encodeAnnotation(message) {
    return message.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}

const findings = [];

for (const lang of readdirSync(localesDir).sort()) {
    const langDir = path.join(localesDir, lang);
    if (!statSync(langDir).isDirectory()) continue;
    for (const file of readdirSync(langDir).sort()) {
        if (!file.endsWith('.json')) continue;
        const filePath = path.join(langDir, file);
        const keys = nestedKeys(JSON.parse(readFileSync(filePath, 'utf-8')));
        if (keys.length > 0) {
            findings.push({ file: path.relative(webReactDir, filePath), keys });
        }
    }
}

if (findings.length === 0) {
    console.log('✅  All locale files use flat keys.');
    process.exit(0);
}

for (const { file, keys } of findings) {
    const message = `${keys.length} nested key(s), flatten them into dotted keys:\n${keys.map((k) => `  - ${k}`).join('\n')}`;
    console.error(`❌  ${file} — ${message}`);
    if (process.env.GITHUB_ACTIONS === 'true') {
        // Paths in annotations are relative to the repository root.
        console.log(
            `::error file=datahub-web-react/${file},title=i18n nested keys::${encodeAnnotation(message)}`,
        );
    }
}

console.error(
    `\n❌  ${findings.length} locale file(s) contain nested keys. Store every key flat, e.g. "a.b.c": "...".`,
);
process.exit(1);
