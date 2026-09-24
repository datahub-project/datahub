import { SimpleSelect } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';
import { v4 as uuidv4 } from 'uuid';

import { Radio } from '@components/components/Radio/Radio';

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
    cursor: pointer;
`;

type Props = {
    selectedValues: (string | number | null)[];
    allowedValues: AllowedValue[];
    selectSingleValue: (value: string | number) => void;
};

export default function SingleSelectInput({ selectSingleValue, allowedValues, selectedValues }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const [groupName] = useState(() => `structured-property-${uuidv4()}`);
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

    return allowedValues.length > 5 ? (
        <SimpleSelect
            width="full"
            placeholder={t('structuredProperty.selectPlaceholder')}
            showSearch
            values={selectedValues.map(String)}
            options={options}
            showDescriptions
            sortSelectedFirst={false}
            showClear={false}
            dataTestId="structured-property-single-select"
            onUpdate={(values) => {
                const selected = options.find((option) => option.value === values?.[0]);
                if (selected) selectSingleValue(selected.originalValue);
            }}
        />
    ) : (
        <Options role="radiogroup" aria-label={t('structuredProperty.selectPlaceholder')}>
            {options.map((option) => (
                <Option key={option.value}>
                    <Radio
                        label={option.label}
                        value={option.value}
                        name={groupName}
                        isChecked={selectedValues.map(String).includes(option.value)}
                        setIsChecked={(isChecked) => {
                            if (isChecked) selectSingleValue(option.originalValue);
                        }}
                    />
                    {option.description && <ValueDescription description={option.description} />}
                </Option>
            ))}
        </Options>
    );
}
