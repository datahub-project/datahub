import { DatePicker, MultiSelectInput, SimpleSelect } from '@components';
import dayjs from 'dayjs';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import { notEmpty } from '@app/entityV2/shared/utils';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';

import { EntityType } from '@types';

type AllowedValueType = {
    value?: {
        stringValue?: string;
        numberValue?: number;
    };
    description?: string;
};

interface Props {
    values: string[];
    valueTypeUrn?: string;
    allowedValues?: AllowedValueType[];
    allowedEntityTypes?: EntityType[];
    onUpdate: (values: string[]) => void;
}

const DATE_FORMAT = 'YYYY-MM-DD';
const INPUT_TYPE_NUMBER = 'number';

const DatePickerContainer = styled.div<{ $isLast: boolean }>`
    margin-bottom: ${(props) => (props.$isLast ? '0' : '8px')};
`;

const PLACEHOLDER_KEYS = {
    DEFAULT: 'privilegeForm.selectValue',
    DATE: 'privilegeForm.datePlaceholder',
    NUMBER: 'privilegeForm.numberPlaceholder',
    URN: 'privilegeForm.selectEntity',
    VALUE: 'privilegeForm.valuePlaceholder',
} as const;

export default function ValueInput({
    values,
    valueTypeUrn,
    allowedValues = [],
    allowedEntityTypes = [],
    onUpdate,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    // URN type: use EntitySearchSelect
    if (valueTypeUrn === URN_TYPE_URN) {
        return (
            <EntitySearchSelect
                selectedUrns={values}
                entityTypes={allowedEntityTypes}
                onUpdate={onUpdate}
                isMultiSelect
                placeholder={t(PLACEHOLDER_KEYS.URN)}
                width="full"
            />
        );
    }

    // String and Number types with allowed values
    if (allowedValues && allowedValues.length > 0) {
        const options = allowedValues
            .map((v) => {
                // Extract value from union type (StringValue, NumberValue, or direct value)
                // Use type guard to preserve falsy values (0, "") while filtering null/undefined
                if (v.value && typeof v.value === 'object') {
                    if ('stringValue' in v.value && notEmpty(v.value.stringValue)) {
                        return String(v.value.stringValue);
                    }
                    if ('numberValue' in v.value && notEmpty(v.value.numberValue)) {
                        return String(v.value.numberValue);
                    }
                } else if (notEmpty(v.value)) {
                    return String(v.value);
                }

                return null;
            })
            .filter((value): value is string => notEmpty(value))
            .map((valueStr) => ({
                value: valueStr,
                label: valueStr,
            }));

        return (
            <SimpleSelect
                options={options}
                values={values}
                onUpdate={(selected: string[]) => onUpdate(selected || [])}
                placeholder={t(PLACEHOLDER_KEYS.DEFAULT)}
                isMultiSelect
                showClear
                width="full"
            />
        );
    }

    // Date type: use DatePicker
    if (valueTypeUrn === DATE_TYPE_URN) {
        const displayValues = values.length > 0 ? values : [''];
        return (
            <>
                {displayValues.map((value, i) => {
                    const dateKey = `date-${value}-${i}`;
                    const dayjsValue = value ? dayjs(value) : null;
                    const isLast = i === displayValues.length - 1;
                    return (
                        <DatePickerContainer key={dateKey} $isLast={isLast}>
                            <DatePicker
                                value={dayjsValue}
                                onChange={(date) => {
                                    const newValues = values.length > 0 ? [...values] : [];
                                    newValues[i] = date ? date.format(DATE_FORMAT) : '';
                                    onUpdate(newValues.filter((v) => v));
                                }}
                                placeholder={t(PLACEHOLDER_KEYS.DATE)}
                            />
                        </DatePickerContainer>
                    );
                })}
            </>
        );
    }

    // Number type: use MultiSelectInput with HTML number input
    if (valueTypeUrn === NUMBER_TYPE_URN) {
        return (
            <MultiSelectInput
                values={values}
                onUpdate={onUpdate}
                placeholder={t(PLACEHOLDER_KEYS.NUMBER)}
                width="full"
                inputType={INPUT_TYPE_NUMBER}
            />
        );
    }

    // Default: String type without allowed values - use MultiSelectInput
    return (
        <MultiSelectInput values={values} onUpdate={onUpdate} placeholder={t(PLACEHOLDER_KEYS.VALUE)} width="full" />
    );
}
