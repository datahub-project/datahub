import { SimpleSelect } from '@components';
import React from 'react';

import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import {
    getFieldTestId,
    isMultipleField,
    prepareSelectValues,
    processFieldAllowedValues,
} from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField } from '@types';

export type AllowedValuesSelectFieldProps = {
    field: ActionWorkflowField;
    values: FieldValue[];
    onChange: (values: FieldValue[]) => void;
    disabled?: boolean;
    isReviewMode?: boolean;
};

export const AllowedValuesSelectField: React.FC<AllowedValuesSelectFieldProps> = ({
    field,
    values,
    onChange,
    disabled = false,
    isReviewMode = false,
}) => {
    const isMultiple = isMultipleField(field);
    const allowedValues = processFieldAllowedValues(field);
    const selectValues = prepareSelectValues(values);

    return (
        <SimpleSelect
            width="fit-content"
            values={selectValues}
            onUpdate={(newValues) => onChange(newValues)}
            placeholder={
                isReviewMode && selectValues.length === 0 ? 'No value provided' : `Select ${field.name.toLowerCase()}`
            }
            isDisabled={disabled}
            isMultiSelect={isMultiple}
            options={allowedValues}
            showClear
            data-testid={getFieldTestId(field.id)}
        />
    );
};
