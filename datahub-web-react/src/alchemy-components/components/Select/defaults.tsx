import { SelectProps } from './types';

export const simpleSelectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    showClear: true,
    width: 255,
    isMultiSelect: false,
    placeholder: 'Select an option ',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};

export const basicSelectDefaults: SelectProps = {
    options: [],
    label: '',
    size: 'md',
    showSearch: false,
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    isMultiSelect: false,
    showClear: false,
    placeholder: 'Select an option',
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};
