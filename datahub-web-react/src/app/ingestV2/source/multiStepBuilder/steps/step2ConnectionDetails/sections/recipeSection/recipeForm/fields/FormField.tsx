import React, { useMemo } from 'react';

import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import { CheckboxField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/CheckboxField';
import { DateField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/DateField';
import { DictField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/DictField';
import { InputField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/InputField';
import { ListField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/ListField';
import { SecretField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SecretField/SecretField';
import { SelectField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SelectField';
import { TextAreaField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/TextAreaField';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function FormField(props: CommonFieldProps) {
    const { field } = props;

    const FieldComponent = useMemo(() => {
        switch (field.type) {
            case FieldType.TEXT:
                return InputField;
            case FieldType.TEXTAREA:
                return TextAreaField;
            case FieldType.BOOLEAN:
                return CheckboxField;
            case FieldType.DATE:
                return DateField;
            case FieldType.DICT:
                return DictField;
            case FieldType.LIST:
                return ListField;
            case FieldType.SELECT:
                return SelectField;
            case FieldType.SECRET:
                return SecretField;
            default:
                console.error('Unknown field type', field.type);
                return InputField;
        }
    }, [field.type]);

    return <FieldComponent {...props} />;
}
