import { Checkbox, SimpleSelect } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import ValueDescription from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/ValueDescription';
import { getStructuredPropertyValue } from '@app/entity/shared/utils';

import { AllowedValue } from '@types';

const Options = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const Option = styled.div`
    display: flex;
    align-items: center;
`;

const OptionLabel = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
`;

type Props = {
    selectedValues: (string | number | null)[];
    allowedValues: AllowedValue[];
    toggleSelectedValue: (value: string | number) => void;
    updateSelectedValues: (values: (string | number | null)[]) => void;
};

export default function MultiSelectInput({
    toggleSelectedValue,
    updateSelectedValues,
    allowedValues,
    selectedValues,
}: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const shouldShowSelectDropdown = allowedValues.length > 5;
    const options = allowedValues.flatMap((allowedValue) => {
        const value = getStructuredPropertyValue(allowedValue.value);
        return value === null
            ? []
            : [
                  {
                      value: String(value),
                      label: String(value),
                      description: allowedValue.description ?? undefined,
                      originalValue: value,
                  },
              ];
    });

    return shouldShowSelectDropdown ? (
        <SimpleSelect
            width="full"
            placeholder={t('structuredProperty.selectPlaceholder')}
            values={selectedValues.map(String)}
            isMultiSelect
            options={options}
            showDescriptions
            sortSelectedFirst={false}
            dataTestId="structured-property-multi-select"
            onUpdate={(values) => {
                const originalValues = values.flatMap((value) => {
                    const selected = options.find((option) => option.value === value);
                    return selected ? [selected.originalValue] : [];
                });
                updateSelectedValues(originalValues);
            }}
        />
    ) : (
        <Options>
            {options.map((option) => (
                <Option key={option.value}>
                    <Checkbox
                        value={option.value}
                        aria-label={option.label}
                        isChecked={selectedValues.map(String).includes(option.value)}
                        setIsChecked={() => toggleSelectedValue(option.originalValue)}
                        justifyContent="flex-start"
                    />
                    <OptionLabel onClick={() => toggleSelectedValue(option.originalValue)}>
                        {option.label}
                        {option.description && <ValueDescription description={option.description} />}
                    </OptionLabel>
                </Option>
            ))}
        </Options>
    );
}
