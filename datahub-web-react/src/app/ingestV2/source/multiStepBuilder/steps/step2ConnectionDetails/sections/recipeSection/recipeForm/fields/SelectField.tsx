import { SimpleSelect } from '@components';
import React from 'react';

import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

import { RecipeFormItem } from './RecipeFormItem';

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
