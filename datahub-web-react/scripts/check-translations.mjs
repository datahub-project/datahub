/**
 * Checks that every key in the EN locale exists (and is non-empty) in all other locales.
 * Exits 1 if any keys are missing so this can be used as a CI gate.
 *
 * Usage:
 *   node scripts/check-translations.mjs
 *   node scripts/check-translations.mjs --lang de     # check one language only
 */

import { readdirSync, readFileSync, existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const localesDir = path.resolve(__dirname, '../src/i18n/locales');
const BASE_LANG = 'en';

const args = process.argv.slice(2);
const langFilter = args.includes('--lang') ? args[args.indexOf('--lang') + 1] : null;

function flattenKeys(obj, prefix = '') {
    return Object.entries(obj).flatMap(([key, value]) => {
        const fullKey = prefix ? `${prefix}.${key}` : key;
        return typeof value === 'object' && value !== null ? flattenKeys(value, fullKey) : [fullKey];
    });
}

const enDir = path.join(localesDir, BASE_LANG);
const namespaces = readdirSync(enDir).filter((f) => f.endsWith('.json'));
const languages = readdirSync(localesDir).filter((l) => l !== BASE_LANG && (!langFilter || l === langFilter));

if (languages.length === 0) {
    console.log(langFilter ? `No locale directory found for "${langFilter}".` : 'No non-EN locales found.');
    process.exit(0);
}

const totalEnKeys = namespaces.reduce((acc, nsFile) => {
    const keys = flattenKeys(JSON.parse(readFileSync(path.join(enDir, nsFile), 'utf-8')));
    return acc + keys.length;
}, 0);

let totalMissing = 0;

for (const lang of languages) {
    const langDir = path.join(localesDir, lang);
    let langMissing = 0;

    for (const nsFile of namespaces) {
        const enKeys = flattenKeys(JSON.parse(readFileSync(path.join(enDir, nsFile), 'utf-8')));
        const otherPath = path.join(langDir, nsFile);

        if (!existsSync(otherPath)) {
            console.error(`\n[${lang}] ${nsFile} — file missing entirely (${enKeys.length} untranslated keys)`);
            langMissing += enKeys.length;
            continue;
        }

        const otherKeys = new Set(flattenKeys(JSON.parse(readFileSync(otherPath, 'utf-8'))));
        const missing = enKeys.filter((k) => !otherKeys.has(k));

        if (missing.length > 0) {
            console.error(`\n[${lang}] ${nsFile} — ${missing.length} missing key(s):`);
            missing.forEach((k) => console.error(`  - ${k}`));
            langMissing += missing.length;
        }
    }

    const coverage = Math.round(((totalEnKeys - langMissing) / totalEnKeys) * 100);
    console.log(`\n[${lang}] coverage: ${langMissing === 0 ? '100%' : `~${coverage}% (${langMissing} key(s) missing)`}`);
    totalMissing += langMissing;
}

if (totalMissing > 0) {
    console.error(`\n❌  ${totalMissing} missing translation key(s) across all locales`);
    process.exit(1);
} else {
    console.log('\n✅  All translation keys are present in all locales');
}
