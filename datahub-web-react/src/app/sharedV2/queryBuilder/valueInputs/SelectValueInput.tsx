import { Select, SelectOption } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('shared.query-builder');
    const selectOptions: SelectOption[] = useMemo(() => {
        return options.map((option) => ({
            value: option.id,
            label: option.displayName,
        }));
    }, [options]);

    const isMultiSelect = mode === 'multiple';

    const hasSelection = (selected?.length ?? 0) > 0;

    return (
        <Select
            values={selected}
            onUpdate={onChangeSelected}
            placeholder={placeholder || t('value.defaultPlaceholder')}
            options={selectOptions}
            isMultiSelect={isMultiSelect}
            selectLabelProps={
                hasSelection
                    ? {
                          variant: 'labeled',
                          label: label ?? t('value.items'),
                      }
                    : undefined
            }
            width="full"
            showClear
        />
    );
}
