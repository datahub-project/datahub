import React from 'react';

import { BasicSelect } from '@components/components/Select/BasicSelect';
import { SelectOption, SelectProps } from '@components/components/Select/types';

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
    disabledValues: undefined,
    showSelectAll: false,
    selectAllLabel: 'Select All',
    showDescriptions: false,
};

export const Select = <OptionType extends SelectOption = SelectOption>({
    options = [],
    label = selectDefaults.label,
    values = [],
    initialValues,
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
    disabledValues = selectDefaults.disabledValues,
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel = selectDefaults.selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    ...props
}: SelectProps<OptionType>) => {
    return (
        <BasicSelect
            options={options}
            size={size}
            label={label}
            values={values}
            initialValues={initialValues}
            showSearch={showSearch}
            isDisabled={isDisabled}
            isReadOnly={isReadOnly}
            isRequired={isRequired}
            onCancel={onCancel}
            onUpdate={onUpdate}
            width={width}
            isMultiSelect={isMultiSelect}
            placeholder={placeholder}
            disabledValues={disabledValues}
            showSelectAll={showSelectAll}
            selectAllLabel={selectAllLabel}
            showDescriptions={showDescriptions}
            {...props}
        />
    );
};
