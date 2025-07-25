import { DatePicker, Input, TextArea } from '@components';
import React from 'react';

import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import { convertFieldValueToMoment } from '@app/workflows/utils/fieldDateHelpers';
import { getFieldPlaceholder, getFieldTestId } from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField, ActionWorkflowFieldValueType } from '@types';

export type SingleValueInputFieldProps = {
    field: ActionWorkflowField;
    value: FieldValue;
    onChange: (value: FieldValue) => void;
    disabled?: boolean;
    isReviewMode?: boolean;
    index?: number;
};

export const SingleValueInputField: React.FC<SingleValueInputFieldProps> = ({
    field,
    value,
    onChange,
    disabled = false,
    isReviewMode = false,
    index,
}) => {
    const inputValue = value || '';
    const placeholder = getFieldPlaceholder(field.name, isReviewMode);
    const testId = getFieldTestId(field.id, undefined, index);

    switch (field.valueType) {
        case ActionWorkflowFieldValueType.String:
            return (
                <Input
                    label=""
                    value={inputValue.toString()}
                    onChange={(e) => onChange(e.target.value)}
                    placeholder={placeholder}
                    disabled={disabled}
                    data-testid={testId}
                />
            );

        case ActionWorkflowFieldValueType.RichText:
            return (
                <TextArea
                    label=""
                    value={inputValue.toString()}
                    onChange={(e) => onChange(e.target.value)}
                    placeholder={placeholder}
                    rows={4}
                    disabled={disabled}
                    data-testid={testId}
                />
            );

        case ActionWorkflowFieldValueType.Number:
            return (
                <Input
                    label=""
                    type="number"
                    value={inputValue.toString()}
                    onChange={(e) => onChange(Number(e.target.value))}
                    placeholder={placeholder}
                    isDisabled={disabled}
                    data-testid={testId}
                />
            );

        case ActionWorkflowFieldValueType.Date: {
            const dateValue = convertFieldValueToMoment(value);

            return (
                <DatePicker
                    value={dateValue}
                    onChange={(date) => onChange(date ? date.toDate() : null)}
                    disabled={disabled}
                    placeholder={isReviewMode ? 'No value provided' : undefined}
                    data-testid={testId}
                />
            );
        }

        default:
            return (
                <Input
                    label=""
                    value={inputValue.toString()}
                    onChange={(e) => onChange(e.target.value)}
                    placeholder={placeholder}
                    disabled={disabled}
                    data-testid={testId}
                />
            );
    }
};
