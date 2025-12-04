import { TextArea } from '@components';
import React from 'react';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function TextAreaField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem
            recipeField={field}
            valuePropName="value"
            getValueFromEvent={(e) => (e.target.value === '' ? null : e.target.value)}
            showHelperText
        >
            <TextArea required={field.required} placeholder={field.placeholder} />
        </RecipeFormItem>
    );
}
