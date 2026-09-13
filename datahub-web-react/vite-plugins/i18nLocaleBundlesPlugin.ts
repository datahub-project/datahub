import fs from 'fs';
import path from 'path';
import type { Plugin } from 'vite';

type LocaleBundle = Record<string, Record<string, unknown>>;

// Keep these IDs identical to `src/i18n/i18nVirtualModules.ts`.
const I18N_LOCALE_LOADERS_ID = 'virtual:i18n-locale-loaders';
const I18N_LOCALE_MODULE_PREFIX = 'virtual:i18n-locale/';
const I18N_LOCALE_UPDATE_EVENT = 'i18n-locale-update';

const LOADERS_RESOLVED_ID = `\0${I18N_LOCALE_LOADERS_ID}`;
const LOCALE_RESOLVED_PREFIX = `\0${I18N_LOCALE_MODULE_PREFIX}`;

export function listLocaleLanguages(localesDir: string): string[] {
    return fs
        .readdirSync(localesDir)
        .filter((name) => fs.statSync(path.join(localesDir, name)).isDirectory())
        .sort();
}

export function buildLocaleBundle(languageDir: string): LocaleBundle {
    return fs.readdirSync(languageDir).reduce<LocaleBundle>((bundle, file) => {
        if (!file.endsWith('.json')) return bundle;
        const namespace = file.slice(0, -'.json'.length);
        return {
            ...bundle,
            [namespace]: JSON.parse(fs.readFileSync(path.join(languageDir, file), 'utf8')),
        };
    }, {});
}

export function i18nLocaleBundlesPlugin(localesDir: string): Plugin {
    return {
        name: 'i18n-locale-bundles',
        resolveId(id) {
            if (id === I18N_LOCALE_LOADERS_ID) return LOADERS_RESOLVED_ID;
            if (id.startsWith(I18N_LOCALE_MODULE_PREFIX)) {
                return `\0${id}`;
            }
            return undefined;
        },
        load(id) {
            if (id === LOADERS_RESOLVED_ID) {
                const languages = listLocaleLanguages(localesDir);
                const entries = languages.map(
                    (lng) =>
                        `    ${JSON.stringify(lng)}: () => import(${JSON.stringify(`${I18N_LOCALE_MODULE_PREFIX}${lng}`)}),`,
                );
                return `export default {\n${entries.join('\n')}\n};\n`;
            }
            if (id.startsWith(LOCALE_RESOLVED_PREFIX)) {
                const lng = id.slice(LOCALE_RESOLVED_PREFIX.length);
                const languageDir = path.join(localesDir, lng);
                fs.readdirSync(languageDir)
                    .filter((file) => file.endsWith('.json'))
                    .forEach((file) => {
                        this.addWatchFile(path.join(languageDir, file));
                    });
                return `export default ${JSON.stringify(buildLocaleBundle(languageDir))};`;
            }
            return undefined;
        },
        configureServer(server) {
            server.watcher.add(localesDir);
        },
        handleHotUpdate({ file, server }) {
            const rel = path.relative(localesDir, file);
            if (!rel || rel.startsWith('..') || path.isAbsolute(rel) || !file.endsWith('.json')) {
                return undefined;
            }
            const lng = rel.split(path.sep)[0];
            const localeModule = server.moduleGraph.getModuleById(`${LOCALE_RESOLVED_PREFIX}${lng}`);
            if (localeModule) {
                server.moduleGraph.invalidateModule(localeModule);
            }
            server.ws.send({ type: 'custom', event: I18N_LOCALE_UPDATE_EVENT, data: { lng } });
            return [];
        },
    };
}
