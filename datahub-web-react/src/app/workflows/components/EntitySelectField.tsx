import React from 'react';

import { EntitySearchSelect } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchSelect';
import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import { getFieldTestId, isMultipleField, prepareUrnValues } from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField } from '@types';

export type EntitySelectFieldProps = {
    field: ActionWorkflowField;
    values: FieldValue[];
    onChange: (values: FieldValue[]) => void;
    disabled?: boolean;
    isReviewMode?: boolean;
};

export const EntitySelectField: React.FC<EntitySelectFieldProps> = ({
    field,
    values,
    onChange,
    disabled = false,
    isReviewMode = false,
}) => {
    const isMultiple = isMultipleField(field);
    const currentUrns = prepareUrnValues(values);

    return (
        <EntitySearchSelect
            selectedUrns={currentUrns}
            entityTypes={field.allowedEntityTypes || []}
            isMultiSelect={isMultiple}
            placeholder={
                isReviewMode && currentUrns.length === 0 ? 'No value provided' : `Select ${field.name.toLowerCase()}`
            }
            onUpdate={(newUrns) => {
                onChange(newUrns);
            }}
            width="full"
            isDisabled={disabled}
            data-testid={getFieldTestId(field.id)}
        />
    );
};
