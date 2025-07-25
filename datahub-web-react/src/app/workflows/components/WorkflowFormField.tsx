import React from 'react';

import { AllowedValuesSelectField } from '@app/workflows/components/AllowedValuesSelectField';
import { EntitySelectField } from '@app/workflows/components/EntitySelectField';
import { MultiValueInputField } from '@app/workflows/components/MultiValueInputField';
import { SingleValueInputField } from '@app/workflows/components/SingleValueInputField';
import { StyledFormItem } from '@app/workflows/components/shared';
import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import { createFieldChangeHandlers, getFieldInputType, isMultipleField } from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField } from '@types';

export type WorkflowFormFieldProps = {
    field: ActionWorkflowField;
    values: FieldValue[];
    error?: string;
    onChange: (values: FieldValue[]) => void;
    disabled?: boolean;
    isReviewMode?: boolean;
};

export const WorkflowFormField: React.FC<WorkflowFormFieldProps> = ({
    field,
    values,
    error,
    onChange,
    disabled = false,
    isReviewMode = false,
}) => {
    const isMultiple = isMultipleField(field);
    const fieldInputType = getFieldInputType(field);

    const renderFieldInput = () => {
        // Handle URN fields
        if (fieldInputType === 'urn') {
            return (
                <EntitySelectField
                    field={field}
                    values={values}
                    onChange={onChange}
                    disabled={disabled}
                    isReviewMode={isReviewMode}
                />
            );
        }

        // Handle fields with predefined allowed values
        if (fieldInputType === 'select') {
            return (
                <AllowedValuesSelectField
                    field={field}
                    values={values}
                    onChange={onChange}
                    disabled={disabled}
                    isReviewMode={isReviewMode}
                />
            );
        }

        // Handle regular input fields
        if (!isMultiple) {
            const { handleSingleChange } = createFieldChangeHandlers(values, onChange, false);

            return (
                <SingleValueInputField
                    field={field}
                    value={values[0] || null}
                    onChange={handleSingleChange}
                    disabled={disabled}
                    isReviewMode={isReviewMode}
                />
            );
        }

        // Handle multiple values for regular input fields
        return (
            <MultiValueInputField
                field={field}
                values={values}
                onChange={onChange}
                disabled={disabled}
                isReviewMode={isReviewMode}
            />
        );
    };

    return (
        <StyledFormItem
            required={field.required}
            rules={[{ required: field.required }]}
            label={field.name}
            tooltip={field.description}
            validateStatus={error ? 'error' : ''}
            hasFeedback={!!error}
            data-testid={`workflow-form-item-${field.id}`}
        >
            {renderFieldInput()}
            {error && <div style={{ color: 'red', marginTop: 4 }}>{error}</div>}
        </StyledFormItem>
    );
};
