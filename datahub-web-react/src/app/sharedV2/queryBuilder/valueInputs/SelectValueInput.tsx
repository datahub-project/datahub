import { Select, SelectOption } from '@components';
import React, { useMemo } from 'react';

import { SelectOption as BuilderSelectOption } from '@app/sharedV2/queryBuilder/builder/property/types/values';

type Props = {
    options: BuilderSelectOption[];
    selected?: string[];
    label?: string;
    mode?: 'multiple' | 'tags';
    placeholder?: string;
    onChangeSelected: (newSelectedIds: string[] | undefined) => void;
};

export default function SelectValueInput({ options, selected, label, mode, placeholder, onChangeSelected }: Props) {
    const selectOptions: SelectOption[] = useMemo(() => {
        return options.map((option) => ({
            value: option.id,
            label: option.displayName,
        }));
    }, [options]);

    const isMultiSelect = mode === 'multiple';

    return (
        <Select
            values={selected}
            onUpdate={onChangeSelected}
            placeholder={placeholder}
            options={selectOptions}
            isMultiSelect={isMultiSelect}
            selectLabelProps={{
                variant: 'labeled',
                label: label ?? 'Items',
            }}
            width="full"
            showClear
        />
    );
}
