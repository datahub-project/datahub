import { Tab, TabButtonsFit } from '@components/components/ButtonTabs/types';

export const tabButtonsDefaults: { fit: TabButtonsFit } = {
    fit: 'fill',
};

export function getInitialActiveTabKey(tabs: Tab[], defaultKey?: string): string | undefined {
    return defaultKey ?? tabs[0]?.key;
}

export function appendRenderedTabKey(renderedKeys: string[], key: string): string[] {
    if (renderedKeys.includes(key)) {
        return renderedKeys;
    }
    return [...renderedKeys, key];
}

export function shouldRenderTabPanel(tabKey: string, activeKey: string | undefined, renderedKeys: string[]): boolean {
    return tabKey === activeKey || renderedKeys.includes(tabKey);
}
