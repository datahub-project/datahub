import { DatePicker } from 'antd';
import React from 'react';

import { RecipeFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/RecipeFormItem';
import { CommonFieldProps } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/types';

export function DateField({ field }: CommonFieldProps) {
    return (
        <RecipeFormItem recipeField={field} showTooltip>
            <DatePicker showTime />
        </RecipeFormItem>
    );
}
