import React from 'react';
import { useTranslation } from 'react-i18next';

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
    disabledValues: undefined,
    showSelectAll: false,
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
    placeholder,
    disabledValues = selectDefaults.disabledValues,
    showSelectAll = selectDefaults.showSelectAll,
    selectAllLabel,
    showDescriptions = selectDefaults.showDescriptions,
    ...props
}: SelectProps<OptionType>) => {
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');
    const resolvedPlaceholder = placeholder ?? t('select.placeholder');
    const resolvedSelectAllLabel = selectAllLabel ?? tc('selectAll');
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
            placeholder={resolvedPlaceholder}
            disabledValues={disabledValues}
            showSelectAll={showSelectAll}
            selectAllLabel={resolvedSelectAllLabel}
            showDescriptions={showDescriptions}
            {...props}
        />
    );
};
