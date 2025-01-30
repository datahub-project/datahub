import React from 'react';
import { IconNames } from '../Icon';

export type SelectSizeOptions = 'sm' | 'md' | 'lg';

export interface SelectOption {
    value: string;
    label: string;
    description?: string;
    icon?: React.ReactNode;
}

export type SelectLabelVariants = 'default' | 'labeled';

export interface SelectProps {
    options: SelectOption[];
    label?: string;
    values?: string[];
    initialValues?: string[];
    onCancel?: () => void;
    onUpdate?: (selectedValues: string[]) => void;
    size?: SelectSizeOptions;
    icon?: IconNames;
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
    showDescriptions?: boolean;
    optionListTestId?: string;
    optionSwitchable?: boolean;
    selectLabelProps?: {
        variant: SelectLabelVariants;
        label: string;
    };
}

export interface SelectStyleProps {
    fontSize?: SelectSizeOptions;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isOpen?: boolean;
    width?: number | 'full';
}

export interface ActionButtonsProps {
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
    variant?: SelectLabelVariants;
    label?: string;
}

export interface SelectLabelVariantProps extends Omit<SelectLabelDisplayProps, 'variant'> {
    selectedOptions: SelectOption[];
}

export interface SearchInputProps extends React.InputHTMLAttributes<HTMLInputElement> {
    fontSize: SelectSizeOptions;
}
