import { Select, SelectOption } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { SelectOption as BuilderSelectOption } from '@app/sharedV2/queryBuilder/builder/property/types/values';

type Props = {
    options: BuilderSelectOption[];
    selected?: string[];
    mode?: 'multiple' | 'tags';
    placeholder?: string;
    onChangeSelected: (newSelectedIds: string[] | undefined) => void;
};

export default function SelectValueInput({ options, selected, mode, placeholder, onChangeSelected }: Props) {
    const { t } = useTranslation('shared.query-builder');
    const selectOptions: SelectOption[] = useMemo(() => {
        return options.map((option) => ({
            value: option.id,
            label: option.displayName,
        }));
    }, [options]);

    const isMultiSelect = mode === 'multiple';

    // Render the chosen values as removable chips (the default variant) rather than the
    // "labeled" variant's field-name-plus-count, since the property is already named in the
    // picker column to the left and the user wants to see the selected values themselves.
    return (
        <Select
            values={selected}
            onUpdate={onChangeSelected}
            placeholder={placeholder || t('value.defaultPlaceholder')}
            options={selectOptions}
            isMultiSelect={isMultiSelect}
            width="full"
            showClear
        />
    );
}
