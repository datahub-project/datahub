import { EDITING_DOCUMENTATION_URL_PARAM, EntityTabPath } from '@app/entityV2/shared/constants';

const GLOSSARY_TERM_TABS = new Set<string>([
    EntityTabPath.SUMMARY,
    EntityTabPath.DOCUMENTATION,
    EntityTabPath.RELATED_ASSETS,
    EntityTabPath.SCHEMA,
    EntityTabPath.RELATED_TERMS,
    EntityTabPath.PROPERTIES,
]);

const LINEAGE_MODE_PARAM = 'is_lineage_mode';

/**
 * Keep the glossary term tab a preview was opened from. Without a tab,
 * EntityTabs redirects to the first enabled tab (Summary, when that tab is shown).
 * Only is_lineage_mode is carried over. Search, filters, and page belong to the current term.
 */
export function glossaryTermUrlPreservingTab(baseUrl: string, pathname: string, activeTabPath: string): string {
    const onGlossaryTermProfile = pathname.split('/').filter(Boolean)[0] === 'glossaryTerm';
    if (!onGlossaryTermProfile || !activeTabPath) {
        return baseUrl;
    }

    const queryIndex = activeTabPath.indexOf('?');
    const tabPath = queryIndex === -1 ? activeTabPath : activeTabPath.slice(0, queryIndex);
    const query = queryIndex === -1 ? '' : activeTabPath.slice(queryIndex);
    if (query.includes(`${EDITING_DOCUMENTATION_URL_PARAM}=`)) {
        return baseUrl;
    }

    let decodedTab = tabPath;
    try {
        decodedTab = decodeURIComponent(tabPath);
    } catch {
        return baseUrl;
    }
    if (!GLOSSARY_TERM_TABS.has(decodedTab)) {
        return baseUrl;
    }
    return `${baseUrl}/${tabPath}${lineageModeQuery(query)}`;
}

function lineageModeQuery(query: string): string {
    if (!query) {
        return '';
    }
    const params = new URLSearchParams(query.startsWith('?') ? query.slice(1) : query);
    const lineageMode = params.get(LINEAGE_MODE_PARAM);
    if (lineageMode === null) {
        return '';
    }
    return `?${LINEAGE_MODE_PARAM}=${encodeURIComponent(lineageMode)}`;
}
