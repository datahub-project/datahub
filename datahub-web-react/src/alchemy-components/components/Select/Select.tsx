import React from 'react';
import { SelectProps } from './types';
import { BasicSelect } from './BasicSelect';

export const selectDefaults: SelectProps = {
    options: [],
    label: '',
    showSearch: false,
    value: undefined,
    size: 'md',
    isDisabled: false,
    isReadOnly: false,
    isRequired: false,
    width: 255,
};

export const Select = ({
    options = selectDefaults.options,
    label = selectDefaults.label,
    value = '',
    onCancel,
    onUpdate,
    showSearch = selectDefaults.showSearch,
    isDisabled = selectDefaults.isDisabled,
    isReadOnly = selectDefaults.isReadOnly,
    isRequired = selectDefaults.isRequired,
    size = selectDefaults.size,
    width = selectDefaults.width,
    ...props
}: SelectProps) => {
    return (
        <BasicSelect
            options={options}
            size={size}
            label={label}
            value={value}
            showSearch={showSearch}
            isDisabled={isDisabled}
            isReadOnly={isReadOnly}
            isRequired={isRequired}
            onCancel={onCancel}
            onUpdate={onUpdate}
            width={width}
            {...props}
        />
    );
};
