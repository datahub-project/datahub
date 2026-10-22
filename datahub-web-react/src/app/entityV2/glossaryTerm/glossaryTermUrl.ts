import { EDITING_DOCUMENTATION_URL_PARAM, EntityTabPath } from '@app/entityV2/shared/constants';

const GLOSSARY_TERM_TABS = new Set<string>([
    EntityTabPath.SUMMARY,
    EntityTabPath.DOCUMENTATION,
    EntityTabPath.RELATED_ASSETS,
    EntityTabPath.SCHEMA,
    EntityTabPath.RELATED_TERMS,
    EntityTabPath.PROPERTIES,
]);

/**
 * Glossary term previews are opened from the term that is already on screen.
 * Keep that term's tab (for example Related Terms) instead of dropping it, which
 * leaves the profile with no selected tab and an empty pane.
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
    return `${baseUrl}/${tabPath}${query}`;
}
