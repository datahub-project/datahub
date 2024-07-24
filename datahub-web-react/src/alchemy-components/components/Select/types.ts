export type SelectSizeOptions = 'sm' | 'md' | 'lg';

export interface SelectOption {
    value: string;
    label: string;
}

export interface SelectProps {
    options: SelectOption[];
    label: string;
    value?: string;
    onCancel?: () => void;
    onUpdate?: (selectedValues: string[]) => void;
    size?: SelectSizeOptions;
    showSearch?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    width?: number | 'full';
}

export interface SelectStyleProps {
    fontSize?: SelectSizeOptions;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
}

export interface ActionButtonsProps {
    fontSize?: SelectSizeOptions;
    selectedValue: string;
    isOpen: boolean;
    isDisabled: boolean;
    isReadOnly: boolean;
    handleClearSelection: () => void;
}

export interface SelectLabelDisplayProps {
    selectedValue: string;
    options: SelectOption[];
    placeholder: string;
}

export interface SearchInputProps extends React.InputHTMLAttributes<HTMLInputElement> {
    fontSize: SelectSizeOptions;
}
