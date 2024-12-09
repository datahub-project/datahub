import React from 'react';
import styled from 'styled-components/macro';
import { typography } from '@components/theme';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Select, selectDefaults } from '@src/alchemy-components/components/Select/Select';
import { SimpleSelect } from '@src/alchemy-components';

type Option = {
    label: string;
    value: string;
};

type Props = {
    options: Option[];
    selectedValue: string | undefined;
    onSelect: (value: string) => void;
    placeholder: string;
};

export function AcryAssertionTypeSelect({ options, selectedValue, onSelect }: Props) {
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
            showClear={true}
            isCustomisedLabel
            width={50}
        />
    );
}
