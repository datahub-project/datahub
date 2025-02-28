import React from 'react';
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
    const selectedOption = options.find((option) => option.value === selectedValue) || { label: undefined };

    const displayValue = selectedOption.label ? `Group ${selectedOption.label}` : 'Group';

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
            selectLabelProps={{ label: 'Group', variant: 'labeled' }}
        />
    );
}
