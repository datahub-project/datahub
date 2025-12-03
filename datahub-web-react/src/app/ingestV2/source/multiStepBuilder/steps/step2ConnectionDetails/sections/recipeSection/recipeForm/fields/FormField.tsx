import React, { useMemo } from 'react';

import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import { DictField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/DictField';
import { ListField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/ListField';
import { SelectField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/SelectField';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

import { CheckboxField } from './CheckboxField';
import { DateField } from './DateField';
import { InputField } from './InputField';
import { TextAreaField } from './TextAreaField';
import { SecretField } from './SecretField/SecretField';

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
