import fs from 'fs';
import path from 'path';

import type { LocaleBundle } from './i18nVirtualModules';

export function listLocaleLanguages(localesDir: string): string[] {
    return fs
        .readdirSync(localesDir)
        .filter((name) => fs.statSync(path.join(localesDir, name)).isDirectory())
        .sort();
}

export function buildLocaleBundle(languageDir: string): LocaleBundle {
    const bundle: LocaleBundle = {};
    for (const file of fs.readdirSync(languageDir)) {
        if (!file.endsWith('.json')) continue;
        const namespace = file.slice(0, -'.json'.length);
        bundle[namespace] = JSON.parse(fs.readFileSync(path.join(languageDir, file), 'utf8'));
    }
    return bundle;
}
