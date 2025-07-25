import { Button } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SingleValueInputField } from '@app/workflows/components/SingleValueInputField';
import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import { createFieldChangeHandlers } from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField } from '@types';

const MultiValueContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const MultiValueRow = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const RemoveButton = styled(Button)`
    flex-shrink: 0;
`;

const AddButton = styled(Button)`
    align-self: flex-start;
`;

const NoValuesPlaceholder = styled.div`
    color: #999;
    font-style: italic;
`;

export type MultiValueInputFieldProps = {
    field: ActionWorkflowField;
    values: FieldValue[];
    onChange: (values: FieldValue[]) => void;
    disabled?: boolean;
    isReviewMode?: boolean;
};

export const MultiValueInputField: React.FC<MultiValueInputFieldProps> = ({
    field,
    values,
    onChange,
    disabled = false,
    isReviewMode = false,
}) => {
    const { handleMultipleChange, addValue, removeValue } = createFieldChangeHandlers(values, onChange, true);

    // Helper function for test IDs
    const getTestId = (suffix: string) => `workflow-field-${field.id}-${suffix}`;

    return (
        <MultiValueContainer data-testid={getTestId('container')}>
            {values.map((value, index) => (
                <MultiValueRow key={index /* eslint-disable-line react/no-array-index-key */}>
                    <SingleValueInputField
                        field={field}
                        value={value}
                        onChange={(newValue) => handleMultipleChange(index, newValue)}
                        disabled={disabled}
                        isReviewMode={isReviewMode}
                        index={index}
                    />
                    {!disabled && (
                        <RemoveButton
                            size="sm"
                            variant="text"
                            color="primary"
                            onClick={() => removeValue(index)}
                            disabled={disabled}
                            data-testid={`workflow-field-${field.id}-remove-${index}`}
                        >
                            Remove
                        </RemoveButton>
                    )}
                </MultiValueRow>
            ))}
            {!disabled && (
                <AddButton
                    size="sm"
                    variant="text"
                    icon={{ icon: 'Plus', source: 'phosphor' }}
                    onClick={addValue}
                    disabled={disabled}
                    data-testid={getTestId('add')}
                >
                    Add
                </AddButton>
            )}
            {/* Show placeholder when no values and in review mode */}
            {isReviewMode && values.length === 0 && <NoValuesPlaceholder>No values provided</NoValuesPlaceholder>}
        </MultiValueContainer>
    );
};
