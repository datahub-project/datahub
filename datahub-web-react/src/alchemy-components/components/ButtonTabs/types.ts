import React from 'react';

/** Item for the tab switcher chrome only (no panel content). */
export type TabButtonItem = {
    key: string;
    label: React.ReactNode;
    dataTestId?: string;
};

/** Full tab used by `ButtonTabs` (chrome + lazily kept panel content). */
export type Tab = TabButtonItem & {
    content: React.ReactNode;
};

export type TabButtonsFit = 'fill' | 'hug';

export type TabButtonsProps = {
    tabs: TabButtonItem[];
    activeTab: string | undefined;
    onTabClick: (key: string) => void;
    /**
     * `fill` — tabs stretch equally (text switches in forms / lineage).
     * `hug` — tabs size to content (icon-only switches like schema raw/table).
     */
    fit?: TabButtonsFit;
    className?: string;
    'data-testid'?: string;
    'aria-label'?: string;
};

export type ButtonTabsProps = {
    tabs: Tab[];
    defaultKey?: string;
    onTabClick?: (key: string) => void;
    fit?: TabButtonsFit;
    className?: string;
};
