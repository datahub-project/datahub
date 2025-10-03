import { Select, SelectOption } from '@components';
import React, { useMemo } from 'react';

import { SelectOption as BuilderSelectOption } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { mergeArraysOfObjects } from '@app/utils/arrayUtils';

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
        const unsortedOptions = options.map((option) => ({
            value: option.id,
            label: option.displayName,
        }));

        return mergeArraysOfObjects(
            unsortedOptions.filter((option) => selected?.includes(option.value)),
            unsortedOptions.filter((option) => !selected?.includes(option.value)),
            (option) => option.value,
            true,
        );
    }, [options, selected]);

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
            autoUpdate
        />
    );
}
