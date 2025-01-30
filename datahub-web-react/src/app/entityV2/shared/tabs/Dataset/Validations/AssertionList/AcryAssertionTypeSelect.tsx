import { SimpleSelect } from '@src/alchemy-components';
import React from 'react';

type Option = {
    label: string;
    value: string;
};

type Props = {
    options: Option[];
    selectedValue: string | undefined;
    onSelect: (value: string) => void;
};

export function AcryAssertionTypeSelect({ options, selectedValue, onSelect }: Props) {
    const selectedOption = options.find((option) => option.value === selectedValue) || { label: undefined };

    const displayValue = selectedOption.label ? `Group ${selectedOption.label}` : 'Group';

    return (
        <SimpleSelect
            options={options}
            values={selectedValue ? [selectedValue] : []}
            selectLabelProps={{ variant: 'labeled', label: 'Group' }}
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
            optionSwitchable
            width={50}
        />
    );
}
