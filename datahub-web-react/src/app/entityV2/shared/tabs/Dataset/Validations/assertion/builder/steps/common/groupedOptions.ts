// Shared helpers for building grouped options for AntD Select

export type SelectOption = { label: string; value: string | number; disabled?: boolean };
export type SelectOptionGroup = { label: string; options: SelectOption[] };

export const groupOptions = (sections: Array<[string, SelectOption[]]>): SelectOptionGroup[] => {
    return sections
        .filter(([, opts]) => Array.isArray(opts) && opts.length > 0)
        .map(([label, options]) => ({ label, options }));
};

// Standard section labels for assertions
export const SECTION_LABELS = {
    anomalyDetection: 'Anomaly Detection',
    value: 'Value',
    growthRate: 'Growth Rate',
    rowCount: 'Row Count',
} as const;
