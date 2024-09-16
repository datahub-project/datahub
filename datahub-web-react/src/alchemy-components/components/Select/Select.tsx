import React from 'react';
import { BasicSelect } from './BasicSelect';
import { SelectProps } from './types';

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    showSearch: false,
    values: undefined,
    size: 'md',
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    width: 255,
    isMultiSelect: false,
    placeholder: 'Select an option',
};

export const Select = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    values = [],
    onCancel,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    size = selectDefaults.size,
    width = selectDefaults.width,
    isMultiSelect = selectDefaults.isMultiSelect,
    placeholder = selectDefaults.placeholder,
    ...props
}: SelectProps) => {
    return (
        <BasicSelect
            options={options}
            size={size}
            label={label}
            values={values}
            showSearch={showSearch}
            isDisabled={isDisabled}
            isReadOnly={isReadOnly}
            isRequired={isRequired}
            onCancel={onCancel}
            onUpdate={onUpdate}
            width={width}
            isMultiSelect={isMultiSelect}
            placeholder={placeholder}
            {...props}
        />
    );
};
