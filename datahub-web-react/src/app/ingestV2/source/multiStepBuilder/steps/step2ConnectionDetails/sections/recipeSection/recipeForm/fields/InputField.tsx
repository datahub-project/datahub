import { Input } from '@components';
import React from 'react';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function InputField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            valuePropName="value"
            getValueFromEvent={(e) => (e.target.value === '' ? null : e.target.value)}
            showHelperText
        >
            <Input required={field.required} isRequired={field.required} placeholder={field.placeholder} />
        </RecipeFormItem>
    );
}
