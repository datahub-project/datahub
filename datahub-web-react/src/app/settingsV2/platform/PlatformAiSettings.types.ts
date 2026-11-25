/**
 * Tab model, URLs and URL matching for the Platform AI Settings page.
 */
import type { ReactNode } from 'react';

export const AI_SETTINGS_BASE_PATH = '/settings/ai';

/** Tabs DataHub builds in. Deployments add more through additionalAiSettingsTabs. */
export enum AiTabType {
    McpServers = 'mcp-servers',
}

/** One tab of the AI settings page. */
export type AiSettingsTab = {
    /** URL segment under /settings/ai, e.g. 'mcp-servers'. */
    key: string;
    /** Tab label. */
    name: string;
    /** Page title and subtitle while this tab is selected. */
    title: string;
    subtitle: string;
    component: JSX.Element;
    /** Buttons shown in the page header while this tab is selected. */
    headerActions?: ReactNode;
    /** Tabs are listed in ascending order; the first one is where /settings/ai lands. */
    order: number;
};

export function aiTabUrl(key: string): string {
    return `${AI_SETTINGS_BASE_PATH}/${key}`;
}

/** Whether the path is /settings/ai itself, with no tab selected. */
export function isAiSettingsBasePath(pathname: string): boolean {
    return pathname === AI_SETTINGS_BASE_PATH || pathname === `${AI_SETTINGS_BASE_PATH}/`;
}

/** The tab a path belongs to, sub-paths (/settings/ai/<tab>/...) included. */
export function getAiTabFromPath(pathname: string, tabKeys: string[]): string | undefined {
    return tabKeys.find((key) => pathname === aiTabUrl(key) || pathname.startsWith(`${aiTabUrl(key)}/`));
}

/** Visible tabs in display order. */
export function sortAiSettingsTabs(tabs: AiSettingsTab[]): AiSettingsTab[] {
    return [...tabs].sort((a, b) => a.order - b.order);
}
