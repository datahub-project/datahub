import fs from 'fs';
import path from 'path';
import type { Plugin } from 'vite';

import { buildLocaleBundle, listLocaleLanguages } from './buildLocaleBundle';
import {
    I18N_LOCALE_LOADERS_ID,
    I18N_LOCALE_MODULE_PREFIX,
    I18N_LOCALE_UPDATE_EVENT,
} from './i18nVirtualModules';

const LOADERS_RESOLVED_ID = `\0${I18N_LOCALE_LOADERS_ID}`;
const LOCALE_RESOLVED_PREFIX = `\0${I18N_LOCALE_MODULE_PREFIX}`;

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
                    (lng) => `    ${JSON.stringify(lng)}: () => import(${JSON.stringify(`${I18N_LOCALE_MODULE_PREFIX}${lng}`)}),`,
                );
                return `export default {\n${entries.join('\n')}\n};\n`;
            }
            if (id.startsWith(LOCALE_RESOLVED_PREFIX)) {
                const lng = id.slice(LOCALE_RESOLVED_PREFIX.length);
                const languageDir = path.join(localesDir, lng);
                for (const file of fs.readdirSync(languageDir)) {
                    if (file.endsWith('.json')) {
                        this.addWatchFile(path.join(languageDir, file));
                    }
                }
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
