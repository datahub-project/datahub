import React from 'react';

import type { MenuPlacement } from '@components/components/FilterBar/placement';

export type FilterMatchMode = 'all' | 'any';

export type FilterOperator = {
    value: string;
    label: string;
    /** Shown on the chip when the rule has more than one value (e.g. "is any of"). */
    pluralLabel?: string;
    requiresValue?: boolean;
    /** Fewest values the operator takes, e.g. 2 for "is all of" or a range: it is only offered, and a new filter only created, once there are that many. */
    minValues?: number;
};

export type FilterValueOption = {
    value: string;
    label: string;
    description?: string;
    count?: number;
    icon?: React.ReactNode;
    disabled?: boolean;
    /** Nested child options (domains, glossary groups, entity subtypes, etc.). */
    children?: FilterValueOption[];
};

export type FilterValueEditorProps = {
    field: FilterField;
    rule: FilterRule;
    onChange: (rule: FilterRule) => void;
    trigger: React.ReactElement;
    /**
     * Linear-style Add Filter: render the value picker inline (no nested popover).
     * Multi-select drafts via onChange; single-select should call onCommit immediately.
     */
    compose?: {
        onCommit: (rule: FilterRule) => void;
    };
};

export type FilterField = {
    field: string;
    label: string;
    description?: string;
    icon?: React.ReactNode;
    operators: FilterOperator[];
    values?: FilterValueOption[];
    selectedOptions?: FilterValueOption[];
    defaultOperator: string;
    group?: string;
    /** Left out of the add menu, so it only renders rules that already exist. */
    hidden?: boolean;
    selectionMode?: 'single' | 'multiple';
    searchable?: boolean;
    loading?: boolean;
    /** Values are listed but their counts are still on the way: a loader sits where an option's count will be. */
    countsLoading?: boolean;
    hasMore?: boolean;
    showSelectAll?: boolean;
    onSearch?: (query: string) => void;
    onLoadMore?: () => void;
    renderValueOption?: (option: FilterValueOption) => React.ReactNode;
    renderValueEditor?: (props: FilterValueEditorProps) => React.ReactNode;
};

export type FilterRule = {
    id: string;
    field: string;
    operator: string;
    values: string[];
};

export type FilterGroup = {
    id: string;
    match: FilterMatchMode;
    filters: FilterRule[];
    groups?: FilterGroup[];
};

export type FilterBarLabels = {
    addFilter: string;
    addGroup: string;
    all: string;
    any: string;
    back: string;
    chooseValue: string;
    clearAll: string;
    collapse: string;
    expand: string;
    noFilters: string;
    removeFilter: string;
    removeGroup: string;
    searchFilters: string;
    searchValues: string;
    selectAll: string;
    where: string;
};

export type FilterBarProps = {
    value: FilterGroup;
    fields: FilterField[];
    onChange: (value: FilterGroup) => void;
    allowGroups?: boolean;
    /** Where "Add filter" sits: after the chips, or on the row below them beside "Clear all". */
    addFilterPlacement?: 'inline' | 'below';
    /** Where the bar's menus open: below their trigger (or above it near the bottom), or beside it, centred. */
    menuPlacement?: MenuPlacement;
    /** With menus placed to the right, the element they open beside instead of their trigger, e.g. the bar's panel. */
    menuAnchor?: React.RefObject<HTMLElement | null>;
    /** Called when the first of the bar's menus opens and when the last one closes. */
    onMenuOpenChange?: (open: boolean) => void;
    labels?: Partial<FilterBarLabels>;
    maxDepth?: number;
    className?: string;
};
