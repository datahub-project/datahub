import React from 'react';
import { useTranslation } from 'react-i18next';

import { SimpleSelect } from '@src/alchemy-components';

type Option = {
    label: string;
    value: string;
};

type GroupBySelectProps = {
    options: Option[];
    selectedValue: string | undefined;
    onSelect: (value: string) => void;
    width?: number;
};

export function GroupBySelect({ options, selectedValue, onSelect, width = 50 }: GroupBySelectProps) {
    const { t } = useTranslation('entity.shared.selectors');
    const selectedOption = options.find((option) => option.value === selectedValue) || { label: undefined };

    const displayValue = selectedOption.label
        ? t('groupBy.displayValue', { label: selectedOption.label })
        : t('groupBy.group');

    return (
        <SimpleSelect
            options={options}
            values={selectedValue ? [selectedValue] : []}
            onUpdate={(value) => {
                if (value.length) {
                    onSelect(value[0]);
                } else {
                    onSelect('');
                }
            }}
            placeholder={displayValue}
            size="md"
            showClear={false}
            width={width}
            selectLabelProps={{ label: t('groupBy.group'), variant: 'labeled' }}
            optionListTestId="group-by-option-list"
            data-testid="group-by-select-input"
        />
    );
}
