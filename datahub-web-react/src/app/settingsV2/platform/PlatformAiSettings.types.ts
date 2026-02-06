/**
 * Tab types, URL mappings, and utility functions for the Platform AI Settings page.
 */

export enum AiTabType {
    Settings = 'settings',
    Plugins = 'plugins',
}

export const AI_TAB_URL_MAP: Record<AiTabType, string> = {
    [AiTabType.Settings]: '/settings/ai/settings',
    [AiTabType.Plugins]: '/settings/ai/plugins',
};

export const DEFAULT_AI_TAB = AiTabType.Settings;

export const AI_BASE_PATH = '/settings/ai';

/**
 * Determines which tab should be active based on the current URL path.
 * Returns undefined if the path doesn't match any tab URL.
 */
export function getTabFromPath(currentPath: string): AiTabType | undefined {
    const entry = Object.entries(AI_TAB_URL_MAP).find(
        ([, url]) => currentPath === url || currentPath.startsWith(`${url}/`),
    );
    return entry ? (entry[0] as AiTabType) : undefined;
}

/**
 * Checks if the current path is the base AI path (no tab selected).
 */
export function isBasePath(currentPath: string): boolean {
    return currentPath === AI_BASE_PATH || currentPath === `${AI_BASE_PATH}/`;
}

/**
 * Result of determining the active tab from URL.
 */
export interface TabFromUrlResult {
    tab: AiTabType | undefined;
    shouldRedirect: boolean;
    redirectUrl: string | undefined;
}

/**
 * Determines the active tab from URL and whether a redirect is needed.
 * This encapsulates the tab determination logic for the PlatformAiSettings component.
 */
export function determineActiveTabFromUrl(currentPath: string): TabFromUrlResult {
    // Check if we're on a specific tab URL
    const currentTab = getTabFromPath(currentPath);
    if (currentTab) {
        return {
            tab: currentTab,
            shouldRedirect: false,
            redirectUrl: undefined,
        };
    }

    // Check if we're on the base path (need to redirect to default tab)
    if (isBasePath(currentPath)) {
        return {
            tab: DEFAULT_AI_TAB,
            shouldRedirect: true,
            redirectUrl: AI_TAB_URL_MAP[DEFAULT_AI_TAB],
        };
    }

    // Not on any recognized path
    return {
        tab: undefined,
        shouldRedirect: false,
        redirectUrl: undefined,
    };
}
