import fs from 'fs';
import path from 'path';
import type { Plugin } from 'vite';

type LocaleBundle = Record<string, Record<string, unknown>>;
type NamespaceGroups = Record<string, string>;

// Keep these IDs identical to `src/i18n/i18nVirtualModules.ts`.
const I18N_LOCALE_LOADERS_ID = 'virtual:i18n-locale-loaders';
const I18N_LOCALE_MODULE_PREFIX = 'virtual:i18n-locale/';
const I18N_LOCALE_UPDATE_EVENT = 'i18n-locale-update';
export const CORE_NAMESPACE_GROUP = 'core';

const LOADERS_RESOLVED_ID = `\0${I18N_LOCALE_LOADERS_ID}`;
const LOCALE_RESOLVED_PREFIX = `\0${I18N_LOCALE_MODULE_PREFIX}`;

export function listLocaleLanguages(localesDir: string): string[] {
    return fs
        .readdirSync(localesDir)
        .filter((name) => fs.statSync(path.join(localesDir, name)).isDirectory())
        .sort();
}

export function namespaceGroup(namespace: string): string {
    if (
        namespace === 'alchemy' ||
        namespace === 'misc' ||
        namespace === 'search' ||
        namespace === 'entity.types' ||
        namespace === 'home.v2' ||
        namespace.startsWith('common.') ||
        namespace.startsWith('shared.')
    ) {
        return CORE_NAMESPACE_GROUP;
    }
    if (namespace === 'auth' || namespace === 'onboarding') return namespace;
    if (namespace.startsWith('saas.settings.')) return 'settings';
    if (namespace === 'saas.ingestion') return 'ingestion';
    if (namespace === 'saas.tests' || namespace.startsWith('saas.entity.profile.validations')) return 'quality';
    if (namespace.startsWith('saas.context.') || namespace === 'saas.context-hub') return 'context';
    if (namespace === 'saas.automations') return 'automations';
    if (namespace.startsWith('saas.')) return 'saas';
    if (namespace === 'ingest.sources' || namespace.startsWith('ingestion')) return 'ingestion';
    if (namespace.startsWith('settings.')) return 'settings';
    if (namespace.startsWith('governance.') || namespace === 'logicalModels') return 'governance';
    if (namespace === 'home.v3' || namespace === 'modules') return 'home';
    if (namespace.startsWith('entity.profile.validations') || namespace.startsWith('entity.profile.tests')) {
        return 'quality';
    }
    if (namespace.startsWith('entity.') || namespace.startsWith('entityV1.')) return 'entity';
    return namespace;
}

export function listNamespaceGroups(languageDir: string): NamespaceGroups {
    return Object.fromEntries(
        fs
            .readdirSync(languageDir)
            .filter((file) => file.endsWith('.json'))
            .map((file) => {
                const namespace = file.slice(0, -'.json'.length);
                return [namespace, namespaceGroup(namespace)];
            }),
    );
}

function invertNamespaceGroups(namespaceGroups: NamespaceGroups): Record<string, string[]> {
    return Object.entries(namespaceGroups).reduce<Record<string, string[]>>((byGroup, [namespace, group]) => {
        return {
            ...byGroup,
            [group]: [...(byGroup[group] ?? []), namespace],
        };
    }, {});
}

export function buildLocaleBundle(languageDir: string, group?: string): LocaleBundle {
    return fs.readdirSync(languageDir).reduce<LocaleBundle>((bundle, file) => {
        if (!file.endsWith('.json')) return bundle;
        const namespace = file.slice(0, -'.json'.length);
        if (group && namespaceGroup(namespace) !== group) return bundle;
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
                const namespaceGroups = listNamespaceGroups(path.join(localesDir, 'en'));
                const groups = [...new Set(Object.values(namespaceGroups))].sort();
                const entries = languages.map((lng) => {
                    const groupEntries = groups.map(
                        (group) =>
                            `        ${JSON.stringify(group)}: () => import(${JSON.stringify(
                                `${I18N_LOCALE_MODULE_PREFIX}${lng}/${group}`,
                            )}),`,
                    );
                    return `    ${JSON.stringify(lng)}: {\n${groupEntries.join('\n')}\n    },`;
                });
                const coreNamespaces = Object.entries(namespaceGroups)
                    .filter(([, group]) => group === CORE_NAMESPACE_GROUP)
                    .map(([namespace]) => namespace);
                return [
                    `export const namespaceGroups = ${JSON.stringify(namespaceGroups)};`,
                    `export const coreNamespaces = ${JSON.stringify(coreNamespaces)};`,
                    `export const namespacesByGroup = ${JSON.stringify(invertNamespaceGroups(namespaceGroups))};`,
                    `export default {\n${entries.join('\n')}\n};`,
                ].join('\n');
            }
            if (id.startsWith(LOCALE_RESOLVED_PREFIX)) {
                const [lng, group] = id.slice(LOCALE_RESOLVED_PREFIX.length).split('/');
                const languageDir = path.join(localesDir, lng);
                fs.readdirSync(languageDir)
                    .filter(
                        (file) => file.endsWith('.json') && namespaceGroup(file.slice(0, -'.json'.length)) === group,
                    )
                    .forEach((file) => {
                        this.addWatchFile(path.join(languageDir, file));
                    });
                return `export default ${JSON.stringify(buildLocaleBundle(languageDir, group))};`;
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
            const namespace = path.basename(file, '.json');
            const group = namespaceGroup(namespace);
            const localeModule = server.moduleGraph.getModuleById(`${LOCALE_RESOLVED_PREFIX}${lng}/${group}`);
            if (localeModule) {
                server.moduleGraph.invalidateModule(localeModule);
            }
            server.ws.send({ type: 'custom', event: I18N_LOCALE_UPDATE_EVENT, data: { lng, group } });
            return [];
        },
    };
}
