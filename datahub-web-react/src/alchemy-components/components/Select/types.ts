import { DropdownProps } from 'antd';
import React from 'react';

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

interface RenderSelectBaseProps {
    isOpened: boolean;
    onClick: () => void;
}

export interface SelectProps<OptionType extends SelectOption = SelectOption> {
    options: OptionType[];
    label?: string;
    values?: string[];
    initialValues?: string[];
    onCancel?: () => void;
    onClear?: () => void;
    onUpdate?: (selectedValues: string[]) => void;
    /** Label for the confirm button in the dropdown footer. Defaults to "Update". */
    updateLabel?: string;
    onOpenChange?: (isOpen: boolean) => void;
    size?: SelectSizeOptions;
    icon?: React.ComponentType<any>;
    showSearch?: boolean;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    showClear?: boolean;
    width?: number | 'full' | 'fit-content';
    minWidth?: string;
    /** Caps the closed select's rendered width; the selected label truncates with an ellipsis past this width. */
    maxWidth?: number;
    isMultiSelect?: boolean;
    placeholder?: string | React.ReactNode;
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
    visibilityDeps?: React.DependencyList;
    placement?: DropdownProps['placement'];
    /** Open the dropdown on mount (e.g. after "+ Filter" promotes a control). */
    defaultOpen?: boolean;
    renderSelectBase?: (props: RenderSelectBaseProps) => React.ReactElement;
    renderOptionsFooter?: () => React.ReactNode;
    /** When true (default), selected items appear first in the dropdown. Set to false to maintain original option order. */
    sortSelectedFirst?: boolean;
}

export interface SelectStyleProps {
    fontSize?: SelectSizeOptions;
    isDisabled?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
    isOpen?: boolean;
    width?: number | 'full' | 'fit-content';
    maxWidth?: number;
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
    placeholder: string | React.ReactNode;
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
