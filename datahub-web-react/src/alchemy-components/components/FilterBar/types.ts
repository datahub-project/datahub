import React from 'react';

export type FilterMatchMode = 'all' | 'any';

export type FilterOperator = {
    value: string;
    label: string;
    /** Shown on the chip when the rule has more than one value (e.g. "is any of"). */
    pluralLabel?: string;
    requiresValue?: boolean;
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
    selectionMode?: 'single' | 'multiple';
    searchable?: boolean;
    loading?: boolean;
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
    labels?: Partial<FilterBarLabels>;
    maxDepth?: number;
    className?: string;
};
