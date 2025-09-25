import React from 'react';

import { IconNames } from '@components/components/Icon';

export type SelectSizeOptions = 'sm' | 'md' | 'lg';
export interface SelectOption {
    value: string;
    label: string;
    description?: string;
    icon?: React.ReactNode;
}

export type SelectLabelVariants = 'default' | 'labeled' | 'custom';
export type SelectLabelProps = {
    variant: SelectLabelVariants;
    label?: string;
};

type OptionPosition = 'start' | 'end' | 'center';

export type CustomOptionRenderer<OptionType extends SelectOption> = (option: OptionType) => React.ReactNode;

export interface SelectProps<OptionType extends SelectOption = SelectOption> {
    options: OptionType[];
    label?: string;
    values?: string[];
    initialValues?: string[];
    onCancel?: () => void;
    onClear?: () => void;
    onUpdate?: (selectedValues: string[]) => void;
    size?: SelectSizeOptions;
    icon?: IconNames;
    showSearch?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    showClear?: boolean;
    width?: number | 'full' | 'fit-content';
    isMultiSelect?: boolean;
    placeholder?: string;
    disabledValues?: string[];
    showSelectAll?: boolean;
    selectAllLabel?: string;
    showDescriptions?: boolean;
    renderCustomOptionText?: CustomOptionRenderer<OptionType>;
    renderCustomSelectedValue?: (selectedOptions: OptionType) => void;
    filterResultsByQuery?: boolean;
    onSearchChange?: (searchText: string) => void;
    combinedSelectedAndSearchOptions?: OptionType[];
    optionListStyle?: React.CSSProperties;
    selectedOptionListStyle?: React.CSSProperties;
    optionListTestId?: string;
    optionSwitchable?: boolean;
    selectLabelProps?: SelectLabelProps;
    position?: OptionPosition;
    applyHoverWidth?: boolean;
    ignoreMaxHeight?: boolean;
    isLoading?: boolean;
    emptyState?: React.ReactElement;
    descriptionMaxWidth?: number;
    dataTestId?: string;
    autoUpdate?: boolean;
}

export interface SelectStyleProps {
    fontSize?: SelectSizeOptions;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isOpen?: boolean;
    width?: number | 'full' | 'fit-content';
    position?: OptionPosition;
}

export interface ActionButtonsProps {
    hasSelectedValues: boolean;
    isOpen: boolean;
    isDisabled: boolean;
    isReadOnly: boolean;
    showClear: boolean;
    fontSize?: SelectSizeOptions;
    handleClearSelection: () => void;
}

export interface SelectLabelDisplayProps<OptionType extends SelectOption> {
    selectedValues: string[];
    options: OptionType[];
    placeholder: string;
    isMultiSelect?: boolean;
    removeOption?: (option: OptionType) => void;
    disabledValues?: string[];
    showDescriptions?: boolean;
    isCustomisedLabel?: boolean;
    renderCustomSelectedValue?: (selectedOptions: OptionType) => void;
    variant?: SelectLabelVariants;
    label?: string;
    selectedOptionListStyle?: React.CSSProperties;
}

export interface SelectLabelVariantProps<OptionType extends SelectOption>
    extends Omit<SelectLabelDisplayProps<OptionType>, 'variant'> {
    selectedOptions: OptionType[];
}

export interface SearchInputProps extends React.InputHTMLAttributes<HTMLInputElement> {
    fontSize: SelectSizeOptions;
}
