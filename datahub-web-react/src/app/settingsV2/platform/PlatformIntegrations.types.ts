/**
 * Tab types, URL mappings, and utility functions for the Platform Integrations page.
 */

/**
 * Integration item shape for filtering and routing.
 */
export interface IntegrationItem {
    id: string;
    name: string;
    description: string;
    img: string;
    content: React.ReactNode;
}

/**
 * Feature flags relevant to integrations.
 */
export interface IntegrationFeatureFlags {
    teamsNotificationsEnabled?: boolean;
}

export enum IntegrationsTabType {
    Notifications = 'notifications',
    DataIntegrations = 'data',
}

export const INTEGRATIONS_TAB_URL_MAP: Record<IntegrationsTabType, string> = {
    [IntegrationsTabType.Notifications]: '/settings/integrations/notifications',
    [IntegrationsTabType.DataIntegrations]: '/settings/integrations/data',
};

export const DEFAULT_INTEGRATIONS_TAB = IntegrationsTabType.Notifications;

export const INTEGRATIONS_BASE_PATH = '/settings/integrations';

/**
 * Determines which tab should be active based on the current URL path.
 * Returns undefined if the path doesn't match any tab URL.
 */
export function getTabFromPath(currentPath: string): IntegrationsTabType | undefined {
    const entry = Object.entries(INTEGRATIONS_TAB_URL_MAP).find(
        ([, url]) => currentPath === url || currentPath.startsWith(`${url}/`),
    );
    return entry ? (entry[0] as IntegrationsTabType) : undefined;
}

/**
 * Checks if the current path is the base integrations path (no tab selected).
 */
export function isBasePath(currentPath: string): boolean {
    return currentPath === INTEGRATIONS_BASE_PATH || currentPath === `${INTEGRATIONS_BASE_PATH}/`;
}

/**
 * Result of determining the active tab from URL.
 */
export interface TabFromUrlResult {
    tab: IntegrationsTabType | undefined;
    shouldRedirect: boolean;
    redirectUrl: string | undefined;
}

/**
 * Determines the active tab from URL and whether a redirect is needed.
 * This encapsulates the tab determination logic for the PlatformIntegrations component.
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
            tab: DEFAULT_INTEGRATIONS_TAB,
            shouldRedirect: true,
            redirectUrl: INTEGRATIONS_TAB_URL_MAP[DEFAULT_INTEGRATIONS_TAB],
        };
    }

    // Not on any recognized path
    return {
        tab: undefined,
        shouldRedirect: false,
        redirectUrl: undefined,
    };
}

/**
 * Filters notification integrations based on feature flags.
 * Currently handles Teams integration visibility.
 */
export function filterIntegrationsByFeatureFlags<T extends { id: string }>(
    integrations: T[],
    featureFlags: IntegrationFeatureFlags,
): T[] {
    return integrations.filter((integration) => {
        if (integration.id === 'microsoft-teams') {
            return featureFlags.teamsNotificationsEnabled === true;
        }
        return true;
    });
}

/**
 * Checks if the current path is an integration detail page.
 */
export function isIntegrationDetailPage(currentPath: string, integrations: Array<{ id: string }>): boolean {
    return integrations.some((integration) => currentPath.includes(`${INTEGRATIONS_BASE_PATH}/${integration.id}`));
}
