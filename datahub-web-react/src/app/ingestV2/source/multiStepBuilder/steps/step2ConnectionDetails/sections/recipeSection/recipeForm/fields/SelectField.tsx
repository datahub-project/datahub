import { SimpleSelect } from '@components';
import React from 'react';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function SelectField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem recipeField={field} showHelperText>
            <SimpleSelect
                placeholder={field.placeholder}
                options={field.options ?? []}
                showClear={!field.required}
                width="full"
            />
        </RecipeFormItem>
    );
}
