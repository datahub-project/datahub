import React from 'react';

export type FilterMatchMode = 'all' | 'any';

export type FilterOperator = {
    value: string;
    label: string;
    requiresValue?: boolean;
};

export type FilterValueOption = {
    value: string;
    label: string;
    description?: string;
    count?: number;
    icon?: React.ReactNode;
    disabled?: boolean;
};

export type FilterValueEditorProps = {
    field: FilterField;
    rule: FilterRule;
    onChange: (rule: FilterRule) => void;
    trigger: React.ReactElement;
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
    chooseValue: string;
    clearAll: string;
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
