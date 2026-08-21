/**
 * Checks that every key group in the EN locale exists (with a non-empty value) in all other locales.
 * Exits 1 if any groups are missing so this can be used as a CI gate.
 *
 * i18next pluralization is key-group-aware: a leaf like `rowCount_one` / `rowCount_other` is grouped
 * by stripping the trailing CLDR plural suffix. A locale is only flagged for a missing *group*, never
 * for having a different set of plural forms than English — different languages have different plural
 * rules (e.g. Japanese only needs `_other`; Arabic needs `zero`/`one`/`two`/`few`/`many`/`other`).
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

// CLDR plural / ordinal suffixes appended by i18next to plural keys.
const PLURAL_SUFFIX = /_(zero|one|two|few|many|other)$/;

const args = process.argv.slice(2);
const langFilter = args.includes('--lang') ? args[args.indexOf('--lang') + 1] : null;

function flattenEntries(obj, prefix = '') {
    return Object.entries(obj).flatMap(([key, value]) => {
        const fullKey = prefix ? `${prefix}.${key}` : key;
        return typeof value === 'object' && value !== null ? flattenEntries(value, fullKey) : [[fullKey, value]];
    });
}

function groupKey(key) {
    return key.replace(PLURAL_SUFFIX, '');
}

/** Unique plural-group identities present in a locale file (value must be a non-empty string). */
function groupKeys(obj) {
    const groups = new Set();
    for (const [key, value] of flattenEntries(obj)) {
        if (typeof value === 'string' && value.trim().length > 0) {
            groups.add(groupKey(key));
        }
    }
    return groups;
}

const enDir = path.join(localesDir, BASE_LANG);
const namespaces = readdirSync(enDir).filter((f) => f.endsWith('.json'));
const languages = readdirSync(localesDir).filter((l) => l !== BASE_LANG && (!langFilter || l === langFilter));

if (languages.length === 0) {
    console.log(langFilter ? `No locale directory found for "${langFilter}".` : 'No non-EN locales found.');
    process.exit(0);
}

const totalEnGroups = namespaces.reduce((acc, nsFile) => {
    const groups = groupKeys(JSON.parse(readFileSync(path.join(enDir, nsFile), 'utf-8')));
    return acc + groups.size;
}, 0);

let totalMissing = 0;

for (const lang of languages) {
    const langDir = path.join(localesDir, lang);
    let langMissing = 0;

    for (const nsFile of namespaces) {
        const enGroups = groupKeys(JSON.parse(readFileSync(path.join(enDir, nsFile), 'utf-8')));
        const otherPath = path.join(langDir, nsFile);

        if (!existsSync(otherPath)) {
            console.error(`\n[${lang}] ${nsFile} — file missing entirely (${enGroups.size} untranslated key groups)`);
            langMissing += enGroups.size;
            continue;
        }

        const otherGroups = groupKeys(JSON.parse(readFileSync(otherPath, 'utf-8')));
        const missing = [...enGroups].filter((g) => !otherGroups.has(g)).sort();

        if (missing.length > 0) {
            console.error(`\n[${lang}] ${nsFile} — ${missing.length} missing key group(s):`);
            missing.forEach((k) => console.error(`  - ${k}`));
            langMissing += missing.length;
        }
    }

    const coverage = Math.round(((totalEnGroups - langMissing) / totalEnGroups) * 100);
    console.log(
        `\n[${lang}] coverage: ${langMissing === 0 ? '100%' : `~${coverage}% (${langMissing} key group(s) missing)`}`,
    );
    totalMissing += langMissing;
}

if (totalMissing > 0) {
    console.error(`\n❌  ${totalMissing} missing translation key group(s) across all locales`);
    process.exit(1);
} else {
    console.log('\n✅  All translation key groups are present in all locales');
}
