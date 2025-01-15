export type SelectSizeOptions = 'sm' | 'md' | 'lg';

export interface SelectOption {
    value: string;
    label: string;
    description?: string;
}

export interface SelectProps {
    options: SelectOption[];
    label?: string;
    values?: string[];
    onCancel?: () => void;
    onUpdate?: (selectedValues: string[]) => void;
    size?: SelectSizeOptions;
    showSearch?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    showClear?: boolean;
    width?: number | 'full';
    isMultiSelect?: boolean;
    placeholder?: string;
    disabledValues?: string[];
    showSelectAll?: boolean;
    selectAllLabel?: string;
    optionListTestId?: string;
    showDescriptions?: boolean;
}

export interface SelectStyleProps {
    fontSize?: SelectSizeOptions;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isOpen?: boolean;
}

export interface ActionButtonsProps {
    fontSize?: SelectSizeOptions;
    selectedValues: string[];
    isOpen: boolean;
    isDisabled: boolean;
    isReadOnly: boolean;
    showClear: boolean;
    handleClearSelection: () => void;
}

export interface SelectLabelDisplayProps {
    selectedValues: string[];
    options: SelectOption[];
    placeholder: string;
    isMultiSelect?: boolean;
    removeOption?: (option: SelectOption) => void;
    disabledValues?: string[];
    showDescriptions?: boolean;
}

export interface SearchInputProps extends React.InputHTMLAttributes<HTMLInputElement> {
    fontSize: SelectSizeOptions;
}
